#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut tree: CowBTree<String> = CowBTree::new();

        assert!(tree.insert(5, "five".to_string()).is_none());
        assert!(tree.insert(3, "three".to_string()).is_none());
        assert!(tree.insert(7, "seven".to_string()).is_none());

        assert_eq!(tree.get(5), Some(&"five".to_string()));
        assert_eq!(tree.get(3), Some(&"three".to_string()));
        assert_eq!(tree.get(7), Some(&"seven".to_string()));
        assert_eq!(tree.get(1), None);

        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn test_update() {
        let mut tree: CowBTree<String> = CowBTree::new();

        assert!(tree.insert(5, "five".to_string()).is_none());
        assert_eq!(tree.insert(5, "FIVE".to_string()), Some("five".to_string()));
        assert_eq!(tree.get(5), Some(&"FIVE".to_string()));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_cow_semantics() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i * 10);
        }

        let snapshot = tree.clone();

        tree.insert(50, 999);
        tree.insert(200, 2000);

        assert_eq!(snapshot.get(50), Some(&500));
        assert_eq!(snapshot.get(200), None);

        assert_eq!(tree.get(50), Some(&999));
        assert_eq!(tree.get(200), Some(&2000));
    }

    #[test]
    fn test_many_inserts_sequential() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..10_000 {
            tree.insert(i, i * 2);
        }

        assert_eq!(tree.len(), 10_000);

        for i in 0..10_000 {
            assert_eq!(tree.get(i), Some(&(i * 2)), "Failed at key {}", i);
        }
    }

    #[test]
    fn test_random_inserts() {
        let mut tree: CowBTree<i64> = CowBTree::new();
        let keys: Vec<i64> = (0..1000).map(|i| (i * 7919 + 13) % 10000).collect();

        for &k in &keys {
            tree.insert(k, k * 2);
        }

        for &k in &keys {
            assert_eq!(tree.get(k), Some(&(k * 2)), "Failed at key {}", k);
        }
    }

    #[test]
    fn test_iteration() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in [5, 2, 8, 1, 9, 3, 7, 4, 6, 0] {
            tree.insert(i, i * 10);
        }

        let keys: Vec<i64> = tree.keys().collect();
        assert_eq!(keys, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_range() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i);
        }

        let range: Vec<i64> = tree.range(25..75).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 50);
        assert_eq!(range[0], 25);
        assert_eq!(range[49], 74);
    }

    #[test]
    fn test_remove_leaf() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20 {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.remove(10), Some(100));
        assert_eq!(tree.get(10), None);
        assert_eq!(tree.len(), 19);

        assert_eq!(tree.remove(10), None);
    }

    #[test]
    fn test_remove_many() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..1000 {
            tree.insert(i, i);
        }

        for i in (0..1000).step_by(2) {
            assert_eq!(tree.remove(i), Some(i));
        }

        assert_eq!(tree.len(), 500);

        for i in 0..1000 {
            if i % 2 == 0 {
                assert_eq!(tree.get(i), None);
            } else {
                assert_eq!(tree.get(i), Some(&i));
            }
        }
    }

    #[test]
    fn test_get_mut() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        tree.insert(5, 50);
        tree.insert(3, 30);
        tree.insert(7, 70);

        if let Some(v) = tree.get_mut(5) {
            *v = 500;
        }

        assert_eq!(tree.get(5), Some(&500));
        assert_eq!(tree.get(3), Some(&30));
    }

    #[test]
    fn test_cow_with_get_mut() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i);
        }

        let snapshot = tree.clone();

        if let Some(v) = tree.get_mut(50) {
            *v = 999;
        }

        assert_eq!(snapshot.get(50), Some(&50));
        assert_eq!(tree.get(50), Some(&999));
    }

    #[test]
    fn test_memory_size() {
        assert!(std::mem::size_of::<CowBTree<i64>>() <= 24);
    }

    #[test]
    fn test_entry_api() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        let v = tree.entry(1).or_insert(10);
        assert_eq!(*v, 10);
        assert_eq!(tree.get(1), Some(&10));

        let v = tree.entry(1).or_insert(20);
        assert_eq!(*v, 10);

        tree.entry(1).and_modify(|v| *v += 5);
        assert_eq!(tree.get(1), Some(&15));

        for i in 2..100 {
            tree.entry(i).or_insert(i * 10);
        }
        assert_eq!(tree.len(), 99);
        assert_eq!(tree.get(50), Some(&500));

        tree.entry(50).and_modify(|v| *v = 999);
        assert_eq!(tree.get(50), Some(&999));
    }

    #[test]
    fn test_entry_vacant() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        match tree.entry(42) {
            Entry::Occupied(_) => panic!("Expected vacant"),
            Entry::Vacant(v) => {
                assert_eq!(v.key(), 42);
                let val = v.insert(420);
                assert_eq!(*val, 420);
            }
        }
        assert_eq!(tree.get(42), Some(&420));
        assert_eq!(tree.len(), 1);

        tree.insert(10, 100);
        tree.insert(50, 500);

        match tree.entry(30) {
            Entry::Occupied(_) => panic!("Expected vacant"),
            Entry::Vacant(v) => {
                assert_eq!(v.key(), 30);
                v.insert(300);
            }
        }
        assert_eq!(tree.get(30), Some(&300));
        assert_eq!(tree.len(), 4);

        match tree.entry(5) {
            Entry::Occupied(_) => panic!("Expected vacant"),
            Entry::Vacant(v) => {
                v.insert(50);
            }
        }
        match tree.entry(100) {
            Entry::Occupied(_) => panic!("Expected vacant"),
            Entry::Vacant(v) => {
                v.insert(1000);
            }
        }
        assert_eq!(tree.get(5), Some(&50));
        assert_eq!(tree.get(100), Some(&1000));
    }

    #[test]
    fn test_entry_occupied() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i * 10);
        }

        match tree.entry(50) {
            Entry::Occupied(o) => {
                assert_eq!(o.key(), 50);
            }
            Entry::Vacant(_) => panic!("Expected occupied"),
        }

        match tree.entry(50) {
            Entry::Occupied(o) => {
                assert_eq!(*o.get(), 500);
            }
            Entry::Vacant(_) => panic!("Expected occupied"),
        }

        match tree.entry(50) {
            Entry::Occupied(mut o) => {
                *o.get_mut() = 5000;
            }
            Entry::Vacant(_) => panic!("Expected occupied"),
        }
        assert_eq!(tree.get(50), Some(&5000));

        match tree.entry(50) {
            Entry::Occupied(mut o) => {
                let old = o.insert(50000);
                assert_eq!(old, 5000);
            }
            Entry::Vacant(_) => panic!("Expected occupied"),
        }
        assert_eq!(tree.get(50), Some(&50000));

        let val_ref = match tree.entry(50) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(_) => panic!("Expected occupied"),
        };
        *val_ref = 500000;
        assert_eq!(tree.get(50), Some(&500000));
    }

    #[test]
    fn test_entry_cow_semantics() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i);
        }

        let snapshot = tree.clone();

        match tree.entry(50) {
            Entry::Occupied(mut o) => {
                o.insert(999);
            }
            Entry::Vacant(_) => panic!("Expected occupied"),
        }

        match tree.entry(200) {
            Entry::Occupied(_) => panic!("Expected vacant"),
            Entry::Vacant(v) => {
                v.insert(2000);
            }
        }

        assert_eq!(snapshot.get(50), Some(&50));
        assert_eq!(snapshot.get(200), None);
        assert_eq!(snapshot.len(), 100);

        assert_eq!(tree.get(50), Some(&999));
        assert_eq!(tree.get(200), Some(&2000));
        assert_eq!(tree.len(), 101);
    }

    #[test]
    fn test_entry_many_inserts() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..1000 {
            match tree.entry(i) {
                Entry::Occupied(_) => panic!("Should be vacant"),
                Entry::Vacant(v) => {
                    v.insert(i * 10);
                }
            }
        }

        assert_eq!(tree.len(), 1000);

        for i in 0..1000 {
            match tree.entry(i) {
                Entry::Occupied(mut o) => {
                    assert_eq!(*o.get(), i * 10);
                    o.insert(i * 20);
                }
                Entry::Vacant(_) => panic!("Should be occupied"),
            }
        }

        for i in 0..1000 {
            assert_eq!(tree.get(i), Some(&(i * 20)));
        }
    }

    #[test]
    fn test_node_path_limit() {
        let mut path = NodePath::new();
        for i in 0..MAX_TREE_DEPTH {
            path.push(i);
        }
        assert_eq!(path.len, MAX_TREE_DEPTH as u8);
    }

    #[test]
    fn test_entry_sequential_optimization() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        tree.insert(0, 0);

        for i in 1..1000 {
            tree.entry(i).or_insert(i * 10);
        }

        assert_eq!(tree.len(), 1000);
        assert_eq!(tree.get(999), Some(&9990));
        assert_eq!(tree.max_key, 999);
    }

    #[test]
    fn test_drop_is_called() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Clone)]
        #[allow(dead_code)]
        struct DropCounter(i64);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let mut tree: CowBTree<DropCounter> = CowBTree::new();
            for i in 0..100 {
                tree.insert(i, DropCounter(i));
            }
            assert_eq!(tree.len(), 100);
        }

        let drops = DROP_COUNT.load(Ordering::SeqCst);
        assert_eq!(drops, 100, "Expected 100 drops, got {}", drops);
    }

    #[test]
    fn test_drop_with_splits() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Clone)]
        #[allow(dead_code)]
        struct DropCounter(i64);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let mut tree: CowBTree<DropCounter> = CowBTree::new();
            for i in 0..500 {
                tree.insert(i, DropCounter(i));
            }
            assert_eq!(tree.len(), 500);
        }

        let drops = DROP_COUNT.load(Ordering::SeqCst);
        assert_eq!(drops, 500, "Expected 500 drops, got {}", drops);
    }

    #[test]
    fn test_drop_with_clone() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Clone)]
        #[allow(dead_code)]
        struct DropCounter(i64);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let mut tree1: CowBTree<DropCounter> = CowBTree::new();
            for i in 0..100 {
                tree1.insert(i, DropCounter(i));
            }

            let tree2 = tree1.clone();
            assert_eq!(tree1.len(), 100);
            assert_eq!(tree2.len(), 100);

            drop(tree1);
            assert_eq!(
                DROP_COUNT.load(Ordering::SeqCst),
                0,
                "Data should not be dropped while clone exists"
            );
            drop(tree2);
        }

        let drops = DROP_COUNT.load(Ordering::SeqCst);
        assert_eq!(
            drops, 100,
            "Expected 100 drops after both trees dropped, got {}",
            drops
        );
    }

    #[test]
    fn test_rightmost_split_optimization() {
        // Test that sequential inserts use rightmost split optimization
        // which achieves ~100% fill factor instead of 50%
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert enough to cause multiple splits
        // With MAX_KEYS=128, after 1000 inserts we should have ~8 leaf nodes
        // With standard 50% split: ~16 leaf nodes at 50% fill
        // With rightmost split: ~8 leaf nodes at ~100% fill
        for i in 0..1000 {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.len(), 1000);

        // Verify all values are correct
        for i in 0..1000 {
            assert_eq!(tree.get(i), Some(&(i * 10)), "Failed at key {}", i);
        }

        // Count leaf nodes and check fill factor
        let mut leaf_count = 0;
        let mut total_keys = 0;
        for (keys, _) in tree.iter_chunks() {
            leaf_count += 1;
            total_keys += keys.len();

            // All but the last (rightmost) leaf should be nearly full (MAX_KEYS)
            // The rightmost leaf can have 1..=MAX_KEYS keys
            // With rightmost split, non-rightmost leaves have exactly MAX_KEYS
        }

        assert_eq!(total_keys, 1000);

        // With rightmost split optimization:
        // - 1000 keys, MAX_KEYS=128
        // - First 7 leaves should have 128 keys each = 896 keys
        // - Last leaf should have 104 keys
        // - Total: 8 leaves
        //
        // Without optimization (50% split):
        // - Would have ~16 leaves at ~64 keys each

        // We expect 8 leaves (1000 / 128 = 7.8, rounded up)
        // Allow some variance, but should be much less than 16
        assert!(
            leaf_count <= 10,
            "Expected ~8 leaf nodes with rightmost split, got {} (suggests 50% split is being used)",
            leaf_count
        );

        // Verify fill factor: total_keys / (leaf_count * MAX_KEYS) should be > 75%
        let fill_factor = (total_keys as f64) / (leaf_count as f64 * MAX_KEYS as f64);
        assert!(
            fill_factor > 0.75,
            "Expected fill factor > 75%, got {:.1}%",
            fill_factor * 100.0
        );
    }

    #[test]
    fn test_rightmost_split_with_entry_api() {
        // Test that entry API also benefits from rightmost split
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..1000 {
            tree.entry(i).or_insert(i * 10);
        }

        assert_eq!(tree.len(), 1000);

        // Count leaves - should match the insert test
        let leaf_count = tree.iter_chunks().count();
        assert!(
            leaf_count <= 10,
            "Expected ~8 leaf nodes with rightmost split via entry API, got {}",
            leaf_count
        );
    }

    #[test]
    fn test_clear() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i * 10);
        }
        assert_eq!(tree.len(), 100);
        assert!(!tree.is_empty());

        tree.clear();
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.get(50), None);

        // Can insert after clear
        tree.insert(1, 10);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(1), Some(&10));
    }

    #[test]
    fn test_values_iterator() {
        let mut tree: CowBTree<String> = CowBTree::new();

        tree.insert(3, "three".to_string());
        tree.insert(1, "one".to_string());
        tree.insert(2, "two".to_string());

        let values: Vec<&String> = tree.values().collect();
        // Values should be in key-sorted order
        assert_eq!(values, vec!["one", "two", "three"]);
    }

    #[test]
    fn test_range_unbounded() {
        use std::ops::Bound;

        let mut tree: CowBTree<i64> = CowBTree::new();
        for i in 0..100 {
            tree.insert(i, i);
        }

        // Unbounded start: ..50
        let range: Vec<i64> = tree.range(..50).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 50);
        assert_eq!(range[0], 0);
        assert_eq!(range[49], 49);

        // Unbounded end: 50..
        let range: Vec<i64> = tree.range(50..).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 50);
        assert_eq!(range[0], 50);
        assert_eq!(range[49], 99);

        // Fully unbounded: ..
        let range: Vec<i64> = tree.range(..).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 100);

        // RangeInclusive: 25..=75
        let range: Vec<i64> = tree.range(25..=75).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 51);
        assert_eq!(range[0], 25);
        assert_eq!(range[50], 75);

        // Custom bounds with Excluded start
        let range: Vec<i64> = tree
            .range((Bound::Excluded(25), Bound::Included(30)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(range, vec![26, 27, 28, 29, 30]);
    }

    #[test]
    fn test_deletion_triggers_borrow_from_left() {
        // To trigger borrow_from_left, we need:
        // 1. A leaf node that falls below MIN_KEYS after deletion
        // 2. A left sibling with spare keys (more than MIN_KEYS)
        //
        // Strategy: Insert keys to create specific tree structure,
        // then delete from the right leaf to trigger borrowing from left.
        let mut tree: CowBTree<i64> = CowBTree::new();

        // With MAX_KEYS=128, MIN_KEYS=64
        // Insert 200 keys sequentially to get 2 leaves (128 + 72)
        for i in 0..200 {
            tree.insert(i, i);
        }

        // Now delete keys from the second leaf (keys 128-199) until it has < 64 keys
        // We need to delete more than 72 - 64 = 8 keys from the right leaf
        // to trigger underflow. Delete keys 190-199 (10 keys from end).
        for i in 190..200 {
            tree.remove(i);
        }

        // Should still have 190 keys
        assert_eq!(tree.len(), 190);

        // Continue deleting to trigger rebalancing
        // Delete more keys from the second leaf
        for i in 170..190 {
            tree.remove(i);
        }

        // Should have 170 keys
        assert_eq!(tree.len(), 170);

        // Verify tree integrity - all remaining keys should be accessible
        for i in 0..170 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_deletion_triggers_borrow_from_right() {
        // To trigger borrow_from_right, we need:
        // 1. A leaf node that falls below MIN_KEYS after deletion
        // 2. A right sibling with spare keys
        //
        // Strategy: Create tree, then delete from the LEFT leaf
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 200 keys to get 2 leaves
        for i in 0..200 {
            tree.insert(i, i);
        }

        // Delete keys from the first leaf (keys 0-127) to trigger underflow
        // Delete keys 0-70 to make first leaf have < 64 keys
        for i in 0..71 {
            tree.remove(i);
        }

        // Should have 129 keys (200 - 71)
        assert_eq!(tree.len(), 129);

        // Verify tree integrity
        for i in 71..200 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_deletion_triggers_merge() {
        // To trigger merge, we need:
        // 1. A leaf node below MIN_KEYS
        // 2. A sibling also at or below MIN_KEYS (no spare keys to borrow)
        //
        // Strategy: Create minimal tree, delete until both siblings are at MIN_KEYS,
        // then delete one more to trigger merge.
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 150 keys to create 2 leaves with ~75 each after split
        // Actually with rightmost split: first leaf has 128, second has 22
        // Let's insert more to have a more balanced scenario
        for i in 0..300 {
            tree.insert(i, i);
        }

        // Delete many keys to force merges
        // Delete all even keys first
        for i in (0..300).step_by(2) {
            tree.remove(i);
        }

        // Should have 150 keys
        assert_eq!(tree.len(), 150);

        // Delete more to trigger merges
        for i in (1..300).step_by(4) {
            tree.remove(i);
        }

        // Verify remaining keys
        let remaining: Vec<i64> = tree.keys().collect();
        for key in &remaining {
            assert_eq!(tree.get(*key), Some(key));
        }
    }

    #[test]
    fn test_heavy_deletion_rebalancing() {
        // Stress test for deletion rebalancing
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 1000 keys
        for i in 0..1000 {
            tree.insert(i, i);
        }

        // Delete in a pattern that forces various rebalancing scenarios
        // Delete every 3rd key starting from 0
        for i in (0..1000).step_by(3) {
            tree.remove(i);
        }

        // Delete every 3rd key starting from 1
        for i in (1..1000).step_by(3) {
            tree.remove(i);
        }

        // Only keys where i % 3 == 2 remain (333 keys)
        let expected_count = (0..1000).filter(|i| i % 3 == 2).count();
        assert_eq!(tree.len(), expected_count);

        // Verify all remaining keys
        for i in 0..1000 {
            if i % 3 == 2 {
                assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
            } else {
                assert_eq!(tree.get(i), None, "Unexpected key {}", i);
            }
        }
    }

    #[test]
    fn test_delete_all_keys() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..500 {
            tree.insert(i, i);
        }

        // Delete all keys
        for i in 0..500 {
            assert_eq!(tree.remove(i), Some(i), "Failed to remove key {}", i);
        }

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);

        // Can reuse the tree
        tree.insert(1, 1);
        assert_eq!(tree.get(1), Some(&1));
    }

    #[test]
    fn test_remove_from_internal_node() {
        // When removing a key that exists in an internal node (as separator),
        // the algorithm must replace it with predecessor/successor
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert enough to create internal nodes
        for i in 0..500 {
            tree.insert(i, i);
        }

        // Delete keys in the middle that might be internal node separators
        for i in 100..200 {
            assert_eq!(tree.remove(i), Some(i));
        }

        // Verify tree integrity
        for i in 0..100 {
            assert_eq!(tree.get(i), Some(&i));
        }
        for i in 100..200 {
            assert_eq!(tree.get(i), None);
        }
        for i in 200..500 {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_cow_with_deletion() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i, i);
        }

        let snapshot = tree.clone();

        // Delete from original
        for i in 0..50 {
            tree.remove(i);
        }

        // Snapshot should still have all keys
        assert_eq!(snapshot.len(), 100);
        for i in 0..100 {
            assert_eq!(snapshot.get(i), Some(&i));
        }

        // Original should have only keys 50-99
        assert_eq!(tree.len(), 50);
        for i in 0..50 {
            assert_eq!(tree.get(i), None);
        }
        for i in 50..100 {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_entry_key_method() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        tree.insert(10, 100);
        tree.insert(20, 200);

        // Test Entry::key() on occupied entry
        let key = tree.entry(10).key();
        assert_eq!(key, 10);

        // Test Entry::key() on vacant entry
        let key = tree.entry(15).key();
        assert_eq!(key, 15);

        // Verify tree wasn't modified by key() calls
        assert_eq!(tree.len(), 2);
    }

    #[test]
    fn test_and_modify_on_vacant() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        tree.insert(10, 100);

        // and_modify on vacant entry should return Vacant unchanged
        let entry = tree.entry(20).and_modify(|v| *v += 1);
        match entry {
            Entry::Vacant(v) => {
                assert_eq!(v.key(), 20);
                v.insert(200);
            }
            Entry::Occupied(_) => panic!("Expected Vacant after and_modify on non-existent key"),
        }

        assert_eq!(tree.get(20), Some(&200));

        // and_modify on occupied entry should modify the value
        tree.entry(10).and_modify(|v| *v += 1);
        assert_eq!(tree.get(10), Some(&101));
    }

    #[test]
    fn test_entry_api_root_split() {
        // Test that entry API correctly handles root splits
        // This happens when inserting via entry() causes the tree to grow a new level
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert enough keys to cause multiple splits via entry API
        // MAX_KEYS = 128, so after 128 inserts we get first split
        for i in 0..500 {
            tree.entry(i).or_insert(i * 10);
        }

        assert_eq!(tree.len(), 500);

        // Verify all values
        for i in 0..500 {
            assert_eq!(tree.get(i), Some(&(i * 10)));
        }
    }

    #[test]
    fn test_deep_tree_internal_node_split() {
        // Create a tree with 3+ levels to trigger internal node splitting
        // With MAX_KEYS=128, we need more than 128*128 = 16384 keys for 3 levels
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 20000 keys to ensure 3-level tree
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        assert_eq!(tree.len(), 20_000);

        // Verify random samples
        for i in (0..20_000).step_by(100) {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_deep_tree_internal_node_rebalancing() {
        // Create a deep tree and then delete keys to trigger internal node rebalancing
        // This tests borrow_from_left/right and merge operations on internal nodes
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 20000 keys to create 3-level tree
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Delete keys to trigger internal node underflow and rebalancing
        // Delete from the middle to maximize rebalancing
        for i in 5000..15000 {
            tree.remove(i);
        }

        assert_eq!(tree.len(), 10_000);

        // Verify remaining keys
        for i in 0..5000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
        for i in 5000..15000 {
            assert_eq!(tree.get(i), None, "Key {} should be deleted", i);
        }
        for i in 15000..20000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_deep_tree_random_deletion() {
        // Random deletion pattern to exercise various internal node operations
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Delete in a pattern that causes internal node rebalancing
        // Delete every 3rd key
        for i in (0..20_000).step_by(3) {
            tree.remove(i);
        }

        // Delete every 5th key of remaining
        for i in (1..20_000).step_by(5) {
            tree.remove(i);
        }

        // Verify integrity - remaining keys should still be accessible
        let remaining: Vec<i64> = tree.keys().collect();
        for key in &remaining {
            assert_eq!(tree.get(*key), Some(key));
        }
    }

    #[test]
    fn test_deep_tree_cow_semantics() {
        // Test COW semantics with deep tree
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        let snapshot = tree.clone();

        // Modify original
        for i in 0..5000 {
            tree.remove(i);
        }
        tree.insert(25000, 25000);

        // Snapshot should be unchanged
        assert_eq!(snapshot.len(), 20_000);
        assert_eq!(snapshot.get(0), Some(&0));
        assert_eq!(snapshot.get(25000), None);

        // Original should have modifications
        assert_eq!(tree.len(), 15_001);
        assert_eq!(tree.get(0), None);
        assert_eq!(tree.get(25000), Some(&25000));
    }

    #[test]
    fn test_default_trait() {
        let tree: CowBTree<i64> = CowBTree::default();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_empty_tree_range_iteration() {
        let tree: CowBTree<i64> = CowBTree::new();

        // Range on empty tree should return empty iterator
        let range: Vec<_> = tree.range(..).collect();
        assert!(range.is_empty());

        let range: Vec<_> = tree.range(0..100).collect();
        assert!(range.is_empty());

        let range: Vec<_> = tree.range(..50).collect();
        assert!(range.is_empty());

        // iter_chunks on empty tree
        let chunks: Vec<_> = tree.iter_chunks().collect();
        assert!(chunks.is_empty());

        // range_chunks on empty tree
        let chunks: Vec<_> = tree.range_chunks(0..100).collect();
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_random_order_entry_api() {
        // Random order inserts via entry API triggers split_internal (non-rightmost path)
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Use a pseudo-random pattern to insert keys out of order
        let keys: Vec<i64> = (0..1000).map(|i| (i * 7919 + 13) % 10000).collect();

        for &k in &keys {
            tree.entry(k).or_insert(k * 10);
        }

        // Verify all inserted values
        for &k in &keys {
            assert_eq!(tree.get(k), Some(&(k * 10)), "Missing key {}", k);
        }
    }

    #[test]
    fn test_random_order_entry_api_large() {
        // Larger random order inserts to ensure internal node splits
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Generate keys in random order
        let keys: Vec<i64> = (0..5000).map(|i| (i * 7919 + 13) % 50000).collect();

        for &k in &keys {
            tree.entry(k).or_insert(k);
        }

        // Verify all keys are present
        for &k in &keys {
            assert!(tree.contains_key(k), "Missing key {}", k);
        }
    }

    #[test]
    fn test_range_on_deep_tree() {
        // Range iteration on a deep tree exercises seek_to_start internal node path
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Range in the middle of the tree
        let range: Vec<i64> = tree.range(5000..5100).map(|(k, _)| *k).collect();
        assert_eq!(range.len(), 100);
        assert_eq!(range[0], 5000);
        assert_eq!(range[99], 5099);

        // Range with excluded start bound
        use std::ops::Bound;
        let range: Vec<i64> = tree
            .range((Bound::Excluded(10000), Bound::Included(10010)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(range.len(), 10);
        assert_eq!(range[0], 10001);
        assert_eq!(range[9], 10010);

        // Unbounded range on deep tree
        let count = tree.range(..).count();
        assert_eq!(count, 20_000);
    }

    #[test]
    fn test_range_chunks_on_deep_tree() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Range chunks in the middle
        let mut total = 0;
        for (keys, values) in tree.range_chunks(5000..6000) {
            assert_eq!(keys.len(), values.len());
            total += keys.len();
        }
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_deep_tree_delete_from_left_internal() {
        // Delete pattern that triggers borrow_from_right on internal nodes
        // We need to delete from the LEFT side of a deep tree to make left internal
        // nodes underflow while right internal nodes have spare keys
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 20K keys for 3-level tree
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Delete the first 8000 keys - this should cause internal node rebalancing
        // where left internal nodes need to borrow from right siblings
        for i in 0..8000 {
            tree.remove(i);
        }

        assert_eq!(tree.len(), 12_000);

        // Verify remaining keys
        for i in 8000..20_000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_deep_tree_delete_alternating_pattern() {
        // Alternating delete pattern to exercise more internal node paths
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Delete in an alternating pattern across the tree
        // First delete from beginning
        for i in 0..3000 {
            tree.remove(i);
        }

        // Then delete from end
        for i in 17000..20_000 {
            tree.remove(i);
        }

        // Then delete from middle
        for i in 8000..12000 {
            tree.remove(i);
        }

        // Verify deleted keys are gone
        for i in 0..3000 {
            assert_eq!(tree.get(i), None, "Key {} should be deleted", i);
        }
        for i in 17000..20_000 {
            assert_eq!(tree.get(i), None, "Key {} should be deleted", i);
        }
        for i in 8000..12000 {
            assert_eq!(tree.get(i), None, "Key {} should be deleted", i);
        }

        // Verify remaining keys exist
        for i in 3000..8000 {
            assert_eq!(tree.get(i), Some(&i), "Key {} should exist", i);
        }
        for i in 12000..17000 {
            assert_eq!(tree.get(i), Some(&i), "Key {} should exist", i);
        }
    }

    #[test]
    fn test_entry_on_deep_tree_random() {
        // Entry API with random keys on deep tree
        let mut tree: CowBTree<i64> = CowBTree::new();

        // First build a deep tree sequentially
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Now use entry API with random access patterns
        for i in (0..20_000).step_by(7) {
            tree.entry(i).and_modify(|v| *v *= 2);
        }

        // Verify modifications
        for i in 0..20_000 {
            if i % 7 == 0 {
                assert_eq!(tree.get(i), Some(&(i * 2)));
            } else {
                assert_eq!(tree.get(i), Some(&i));
            }
        }
    }

    #[test]
    fn test_iter_on_deep_tree() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // iter() should return all keys in order
        let keys: Vec<i64> = tree.keys().collect();
        assert_eq!(keys.len(), 20_000);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(*k, i as i64);
        }

        // values() should return all values in order
        let values: Vec<&i64> = tree.values().collect();
        assert_eq!(values.len(), 20_000);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(**v, i as i64);
        }
    }

    #[test]
    fn test_get_mut_on_deep_tree() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // get_mut at various positions
        if let Some(v) = tree.get_mut(0) {
            *v = 999;
        }
        if let Some(v) = tree.get_mut(10000) {
            *v = 888;
        }
        if let Some(v) = tree.get_mut(19999) {
            *v = 777;
        }

        assert_eq!(tree.get(0), Some(&999));
        assert_eq!(tree.get(10000), Some(&888));
        assert_eq!(tree.get(19999), Some(&777));
    }

    #[test]
    fn test_contains_key() {
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..100 {
            tree.insert(i * 2, i);
        }

        // Even keys should exist
        for i in 0..100 {
            assert!(tree.contains_key(i * 2));
        }

        // Odd keys should not exist
        for i in 0..100 {
            assert!(!tree.contains_key(i * 2 + 1));
        }
    }

    #[test]
    fn test_reverse_order_inserts() {
        // Reverse order inserts force standard split_internal (not rightmost)
        // because we're always inserting at the leftmost position
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert in reverse order to trigger split_internal (midpoint split)
        for i in (0..5000).rev() {
            tree.insert(i, i);
        }

        assert_eq!(tree.len(), 5000);

        // Verify all keys
        for i in 0..5000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }

        // Verify iteration order
        let keys: Vec<i64> = tree.keys().collect();
        for (idx, &k) in keys.iter().enumerate() {
            assert_eq!(k, idx as i64);
        }
    }

    #[test]
    fn test_shuffled_inserts_large() {
        // Shuffled inserts to trigger standard split_internal paths
        // Using a shuffle pattern that avoids sequential rightmost inserts
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Generate shuffled keys - interleave from both ends
        let mut keys: Vec<i64> = Vec::with_capacity(10000);
        for i in 0..5000 {
            keys.push(i * 2); // Even: 0, 2, 4, ...
            keys.push(i * 2 + 1); // Odd: 1, 3, 5, ...
        }
        // Reverse half to make it more shuffled
        for i in 0..5000 {
            keys.swap(i, 9999 - i);
        }

        for &k in &keys {
            tree.insert(k, k);
        }

        assert_eq!(tree.len(), 10000);

        for i in 0..10000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_internal_node_borrow_from_right() {
        // This test triggers internal node borrow_from_right (the else branch)
        // by creating a 3-level tree and causing internal node underflow
        // where the right sibling is also an internal node (not a leaf)
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Use a shuffle pattern to ensure we don't get the rightmost-optimized splits
        // This creates a more balanced tree with internal nodes that can borrow from each other
        let mut keys: Vec<i64> = (0..50_000).collect();
        // Shuffle using a deterministic pattern
        for i in 0..keys.len() {
            let j = (i * 31337 + 17) % keys.len();
            keys.swap(i, j);
        }

        for &k in &keys {
            tree.insert(k, k);
        }

        assert_eq!(tree.len(), 50_000);

        // Delete a large contiguous region from the left side
        // This should cause leaf merges in that region, which cascade up
        // to cause internal node underflow and borrow/merge operations
        for i in 0..20_000 {
            tree.remove(i);
        }

        assert_eq!(tree.len(), 30_000);

        // Verify remaining keys
        for i in 0..20_000 {
            assert_eq!(tree.get(i), None, "Key {} should be deleted", i);
        }
        for i in 20_000..50_000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_internal_node_merge_cascade() {
        // Trigger internal node merge by deleting enough to cause cascade
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Use reverse interleaving for balanced structure
        for i in 0..30_000 {
            let key = if i % 2 == 0 {
                i / 2
            } else {
                30_000 - 1 - i / 2
            };
            tree.insert(key, key);
        }

        // Delete from multiple regions to trigger internal node merges
        // Delete first third
        for i in 0..10_000 {
            tree.remove(i);
        }

        // Delete last third
        for i in 20_000..30_000 {
            tree.remove(i);
        }

        // Verify remaining keys in middle third
        for i in 10_000..20_000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_massive_deletion_internal_rebalance() {
        // Create large tree and delete 90% of keys to heavily exercise internal node rebalancing
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Interleaved insert pattern
        for i in 0..60_000 {
            let key = (i * 7919) % 60_000;
            tree.insert(key, key);
        }

        // Delete 90% - keep only every 10th key
        for i in 0..60_000 {
            if i % 10 != 0 {
                tree.remove(i);
            }
        }

        assert_eq!(tree.len(), 6000);

        // Verify remaining keys
        for i in (0..60_000).step_by(10) {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_left_heavy_deletion() {
        // Delete heavily from left side to trigger internal node borrow from right
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Build tree with reverse order to create left-biased structure
        for i in (0..22_000).rev() {
            tree.insert(i, i);
        }

        // Delete 80% from left side
        for i in 0..17_600 {
            tree.remove(i);
        }

        assert_eq!(tree.len(), 4400);

        // Verify
        for i in 17_600..22_000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_right_heavy_deletion() {
        // Delete heavily from right side to trigger borrow from left
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Sequential insert for rightmost structure
        for i in 0..30_000 {
            tree.insert(i, i);
        }

        // Delete 80% from right side
        for i in 6000..30_000 {
            tree.remove(i);
        }

        assert_eq!(tree.len(), 6000);

        // Verify
        for i in 0..6000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_alternating_sides_deletion() {
        // Alternate between deleting from left and right to exercise both borrow directions
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..30_000 {
            tree.insert(i, i);
        }

        // Delete from ends alternating
        let mut left = 0;
        let mut right = 29_999;
        for _ in 0..20_000 {
            if left < right {
                tree.remove(left);
                left += 1;
            }
            if left < right {
                tree.remove(right);
                right -= 1;
            }
        }

        // Verify middle section
        for i in left..=right {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_entry_api_reverse_order() {
        // Entry API with reverse order to trigger split_internal (not rightmost)
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in (0..3000).rev() {
            tree.entry(i).or_insert(i * 10);
        }

        assert_eq!(tree.len(), 3000);

        for i in 0..3000 {
            assert_eq!(tree.get(i), Some(&(i * 10)));
        }
    }

    #[test]
    fn test_cow_during_internal_rebalancing() {
        // Test COW semantics are maintained during internal node rebalancing
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Create deep tree
        for i in 0..30_000 {
            tree.insert(i, i);
        }

        // Clone before deletions
        let snapshot = tree.clone();

        // Delete heavily to trigger internal rebalancing
        for i in 0..20_000 {
            tree.remove(i);
        }

        // Snapshot must be intact
        assert_eq!(snapshot.len(), 30_000);
        for i in 0..30_000 {
            assert_eq!(snapshot.get(i), Some(&i), "Snapshot missing key {}", i);
        }

        // Modified tree
        assert_eq!(tree.len(), 10_000);
        for i in 0..20_000 {
            assert_eq!(tree.get(i), None);
        }
        for i in 20_000..30_000 {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_zigzag_insert_pattern() {
        // Insert in zigzag pattern: 0, 9999, 1, 9998, 2, 9997, ...
        // This forces many internal node splits at non-rightmost positions
        let mut tree: CowBTree<i64> = CowBTree::new();

        for i in 0..5000 {
            tree.insert(i, i);
            tree.insert(9999 - i, 9999 - i);
        }

        assert_eq!(tree.len(), 10000);

        for i in 0..10000 {
            assert_eq!(tree.get(i), Some(&i), "Missing key {}", i);
        }
    }

    #[test]
    fn test_middle_insert_split() {
        // Insert at middle positions to force non-rightmost splits
        let mut tree: CowBTree<i64> = CowBTree::new();

        // First insert boundaries
        for i in (0..5000).step_by(100) {
            tree.insert(i, i);
        }

        // Then fill in midpoints repeatedly
        for gap in [50i64, 25, 12, 6, 3, 1] {
            let mut i = gap;
            while i < 5000 {
                if tree.get(i).is_none() {
                    tree.insert(i, i);
                }
                i += gap * 2;
            }
        }

        // Fill any remaining gaps
        for i in 0..5000 {
            if tree.get(i).is_none() {
                tree.insert(i, i);
            }
        }

        assert_eq!(tree.len(), 5000);

        for i in 0..5000 {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_update_separator_keys() {
        // This test triggers the Ok(i) => i + 1 path in insert_recursive
        // by updating keys that are separator keys in internal nodes.
        // Separator keys are promoted during splits.
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert in reverse order to use insert_recursive (not rightmost optimization)
        // This ensures splits go through split_internal, not split_internal_rightmost
        for i in (0..1000).rev() {
            tree.insert(i, i);
        }

        // Now update ALL keys - some of them are separator keys in internal nodes
        // When we update a separator key, search returns Ok(i) and we go to child i+1
        for i in 0..1000 {
            let old = tree.insert(i, i * 100);
            assert_eq!(old, Some(i), "Key {} should have existed", i);
        }

        // Verify updates
        for i in 0..1000 {
            assert_eq!(tree.get(i), Some(&(i * 100)));
        }
    }

    #[test]
    fn test_reverse_insert_internal_node_split() {
        // This test ensures split_internal() (not split_internal_rightmost) is called
        // by inserting in reverse order with enough keys to overflow internal nodes.
        // With MAX_KEYS=128, we need 128*128 = 16384+ keys for 3 levels.
        let mut tree: CowBTree<i64> = CowBTree::new();

        // Insert 20K keys in reverse order
        // All inserts go through insert_recursive since key < max_key
        for i in (0..20_000).rev() {
            tree.insert(i, i);
        }

        assert_eq!(tree.len(), 20_000);

        // Verify all keys
        for i in (0..20_000).step_by(100) {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_iter_rev() {
        let mut tree: CowBTree<i64> = CowBTree::new();
        for i in 0..500 {
            tree.insert(i, i * 10);
        }

        // iter_rev should yield all entries in descending key order
        let rev: Vec<(i64, i64)> = tree.iter_rev().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(rev.len(), 500);
        for (i, &(k, v)) in rev.iter().enumerate() {
            assert_eq!(k, 499 - i as i64);
            assert_eq!(v, k * 10);
        }

        // iter_rev on empty tree
        let empty: CowBTree<i64> = CowBTree::new();
        assert_eq!(empty.iter_rev().count(), 0);
    }

    #[test]
    fn test_range_rev() {
        let mut tree: CowBTree<i64> = CowBTree::new();
        for i in 0..500 {
            tree.insert(i, i * 10);
        }

        // range_rev with Excluded start, Unbounded end (keyset pagination pattern)
        let result: Vec<(i64, i64)> = tree
            .range_rev((std::ops::Bound::Excluded(100), std::ops::Bound::Unbounded))
            .map(|(&k, &v)| (k, v))
            .collect();
        assert_eq!(result.len(), 399); // 101..499 inclusive
        assert_eq!(result[0], (499, 4990)); // Largest first
        assert_eq!(result[398], (101, 1010)); // Smallest last

        // range_rev with Included start, Unbounded end
        let result: Vec<(i64, i64)> = tree
            .range_rev((std::ops::Bound::Included(100), std::ops::Bound::Unbounded))
            .map(|(&k, &v)| (k, v))
            .collect();
        assert_eq!(result.len(), 400); // 100..499 inclusive
        assert_eq!(result[0], (499, 4990));
        assert_eq!(result[399], (100, 1000));

        // range_rev with both bounds
        let result: Vec<(i64, i64)> = tree
            .range_rev((
                std::ops::Bound::Included(200),
                std::ops::Bound::Excluded(300),
            ))
            .map(|(&k, &v)| (k, v))
            .collect();
        assert_eq!(result.len(), 100); // 200..299 inclusive
        assert_eq!(result[0], (299, 2990));
        assert_eq!(result[99], (200, 2000));

        // range_rev with Included end
        let result: Vec<(i64, i64)> = tree
            .range_rev((
                std::ops::Bound::Included(200),
                std::ops::Bound::Included(300),
            ))
            .map(|(&k, &v)| (k, v))
            .collect();
        assert_eq!(result.len(), 101); // 200..=300 inclusive
        assert_eq!(result[0], (300, 3000));
        assert_eq!(result[100], (200, 2000));

        // range_rev unbounded both sides = iter_rev
        let all_rev: Vec<i64> = tree
            .range_rev((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded::<i64>,
            ))
            .map(|(&k, _)| k)
            .collect();
        let iter_rev: Vec<i64> = tree.iter_rev().map(|(&k, _)| k).collect();
        assert_eq!(all_rev, iter_rev);
    }

    #[test]
    fn test_iter_rev_on_deep_tree() {
        let mut tree: CowBTree<i64> = CowBTree::new();
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // iter_rev should return all keys in reverse order
        let rev_keys: Vec<i64> = tree.iter_rev().map(|(&k, _)| k).collect();
        assert_eq!(rev_keys.len(), 20_000);
        for (i, &k) in rev_keys.iter().enumerate() {
            assert_eq!(k, 19_999 - i as i64);
        }

        // Taking a small prefix is O(limit), not O(n)
        let first_5: Vec<i64> = tree.iter_rev().map(|(&k, _)| k).take(5).collect();
        assert_eq!(first_5, vec![19_999, 19_998, 19_997, 19_996, 19_995]);
    }

    #[test]
    fn test_range_rev_on_deep_tree() {
        let mut tree: CowBTree<i64> = CowBTree::new();
        for i in 0..20_000 {
            tree.insert(i, i);
        }

        // Keyset pagination pattern: WHERE id > 15000 ORDER BY id DESC LIMIT 5
        let page: Vec<i64> = tree
            .range_rev((
                std::ops::Bound::Excluded(15_000),
                std::ops::Bound::Unbounded,
            ))
            .map(|(&k, _)| k)
            .take(5)
            .collect();
        assert_eq!(page, vec![19_999, 19_998, 19_997, 19_996, 19_995]);

        // Keyset pagination: WHERE id > 5000 ORDER BY id DESC LIMIT 5
        let page: Vec<i64> = tree
            .range_rev((std::ops::Bound::Excluded(5_000), std::ops::Bound::Unbounded))
            .map(|(&k, _)| k)
            .take(5)
            .collect();
        assert_eq!(page, vec![19_999, 19_998, 19_997, 19_996, 19_995]);

        // Bounded range in reverse
        let result: Vec<i64> = tree
            .range_rev((
                std::ops::Bound::Included(10_000),
                std::ops::Bound::Excluded(10_010),
            ))
            .map(|(&k, _)| k)
            .collect();
        assert_eq!(
            result,
            vec![10_009, 10_008, 10_007, 10_006, 10_005, 10_004, 10_003, 10_002, 10_001, 10_000]
        );
    }
}
