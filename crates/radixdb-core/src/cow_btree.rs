// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Copy-on-Write B-tree for i64 keys
//!
//! Design:
//! - Each node is wrapped in CompactArc (8 bytes) for memory management
//! - Nodes maintain their own `drop_count` (AtomicU32) for thread-safe V drops
//! - Readers clone the root (atomic, ~1ns) and traverse without locks
//! - Writers check `drop_count` and deep-clone only if shared (COW)
//! - Structural sharing: unmodified subtrees are shared between versions
//!
//! Thread safety:
//! - `drop_count` uses atomic fetch_sub to coordinate drops across threads
//! - Exactly one thread (whoever sees old_count=1) drops V values
//! - This avoids race conditions that could cause memory leaks
//!
//! Performance characteristics:
//! - Reads: O(log n), completely lock-free
//! - Writes: O(log n) + O(B) per modified node for COW
//! - Clone: O(1) - just increments reference counts
//! - Memory: Shared nodes between snapshots

use super::CompactArc;
use std::marker::PhantomData;
use std::mem;
use std::ops::Bound;
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};

/// Maximum keys per node. Smaller = more nodes but faster COW.
/// 128 gives good balance for database workloads.
///
/// CONSTRAINT: MAX_KEYS <= 255 because NodePath uses u8 for child indices.
/// Internal nodes have MAX_KEYS + 1 children, so index can be at most MAX_KEYS.
const MAX_KEYS: usize = 128;

// Compile-time assertion: MAX_KEYS must fit in u8 (NodePath uses [u8; MAX_TREE_DEPTH])
const _: () = assert!(
    MAX_KEYS <= 255,
    "MAX_KEYS must be <= 255 (NodePath uses u8 indices)"
);

/// Minimum keys per node (except root).
/// Must satisfy: 2 * MIN_KEYS + 1 <= MAX_KEYS (merge invariant).
const MIN_KEYS: usize = (MAX_KEYS - 1) / 2;

/// Header: 8 bytes
/// [ len (u16) | is_leaf (u8) | pad (1) | drop_count (4) ]
///
/// `drop_count` is a separate refcount that coordinates dropping V values.
/// This fixes a race condition where multiple threads could skip dropping
/// V values when relying solely on CompactArc's is_unique() check.
#[repr(C)]
struct NodeHeader {
    len: u16,
    is_leaf: u8,
    _pad1: u8,
    /// Refcount for coordinating V/children drops.
    /// Decremented atomically in NodePtr::drop; whoever sees old_count=1 drops contents.
    /// Using U32 to support up to ~4 billion concurrent snapshots.
    drop_count: AtomicU32,
}

/// A Ref-counted pointer to a Byte-Packed Node.
/// Memory Layout: [ Header | Keys (MAX_KEYS) | Values/Children ]
/// One contiguous allocation.
pub struct NodePtr<V: Clone> {
    ptr: CompactArc<[u8]>,
    _marker: PhantomData<V>,
}

impl<V: Clone> Clone for NodePtr<V> {
    fn clone(&self) -> Self {
        // Clone CompactArc first (keeps memory alive), then increment drop_count.
        // This ordering ensures drop_count is only incremented after the clone succeeds.
        let new_ptr = self.ptr.clone();

        // SAFETY: ptr points to a valid CompactArc allocation that starts with NodeHeader.
        // The header is read-only here (only accessing drop_count atomically).
        let header = unsafe { &*(self.ptr.data_ptr_mut() as *const NodeHeader) };
        header.drop_count.fetch_add(1, Ordering::Relaxed);

        Self {
            ptr: new_ptr,
            _marker: PhantomData,
        }
    }
}

impl<V: Clone> Drop for NodePtr<V> {
    fn drop(&mut self) {
        // Atomically decrement drop_count and check if we're the last reference.
        // This fixes a race condition where multiple threads using is_unique()
        // could all see refcount > 1 and skip dropping V values.
        //
        // With atomic fetch_sub, exactly one thread will see old_count == 1
        // and that thread is responsible for dropping the contents.
        // SAFETY: ptr points to a valid CompactArc allocation that starts with NodeHeader.
        let header = unsafe { &*(self.ptr.data_ptr_mut() as *const NodeHeader) };
        let old_count = header.drop_count.fetch_sub(1, Ordering::AcqRel);

        if old_count != 1 {
            // Not the last reference - don't drop contents
            return;
        }

        // We're the last reference - drop contents
        let len = self.len();
        if self.is_leaf() {
            // Drop values in valid range
            // SAFETY: We are the last reference (old_count == 1). The values at indices
            // 0..len are valid initialized V instances. After dropping, we don't access them.
            unsafe {
                let v_ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
                for i in 0..len {
                    ptr::drop_in_place(v_ptr.add(i));
                }
            }
        } else {
            // Drop children (keys count + 1)
            // SAFETY: We are the last reference (old_count == 1). The children at indices
            // 0..=len are valid NodePtr instances. After dropping, we don't access them.
            unsafe {
                let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
                for i in 0..=len {
                    ptr::drop_in_place(c_ptr.add(i));
                }
            }
        }
    }
}

impl<V: Clone> NodePtr<V> {
    /// Keys start immediately after the header
    const fn keys_offset() -> usize {
        mem::size_of::<NodeHeader>()
    }

    /// Offset to values array (leaf nodes only).
    /// Same as `children_offset` - this is union-style memory sharing:
    /// leaf nodes store values here, internal nodes store children here.
    /// A node is either leaf OR internal, never both.
    const fn values_offset() -> usize {
        Self::keys_offset() + ((MAX_KEYS + 1) * 8)
    }

    /// Offset to children array (internal nodes only).
    /// Same as `values_offset` - see comment above.
    const fn children_offset() -> usize {
        Self::keys_offset() + ((MAX_KEYS + 1) * 8)
    }

    fn new_leaf() -> Self {
        let size = Self::values_offset() + ((MAX_KEYS + 1) * mem::size_of::<V>());
        let vec = vec![0u8; size];

        let mut ptr = NodePtr {
            ptr: CompactArc::from_vec(vec),
            _marker: PhantomData,
        };

        let header = ptr.header_mut();
        header.len = 0;
        header.is_leaf = 1;
        header.drop_count = AtomicU32::new(1);

        ptr
    }

    fn new_internal() -> Self {
        let size = Self::children_offset() + ((MAX_KEYS + 2) * mem::size_of::<NodePtr<V>>());
        let vec = vec![0u8; size];

        let mut ptr = NodePtr {
            ptr: CompactArc::from_vec(vec),
            _marker: PhantomData,
        };

        let header = ptr.header_mut();
        header.len = 0;
        header.is_leaf = 0;
        header.drop_count = AtomicU32::new(1);

        ptr
    }

    fn make_mut(&mut self) -> &mut Self {
        // Check if this node is shared using our own drop_count.
        // If drop_count > 1, we need to deep clone to maintain COW semantics.
        // SAFETY: ptr points to a valid CompactArc allocation that starts with NodeHeader.
        let header = unsafe { &*(self.ptr.data_ptr_mut() as *const NodeHeader) };
        if header.drop_count.load(Ordering::Acquire) != 1 {
            // Shared - need proper deep clone (not just byte copy!)
            // Byte copy would cause double-free for non-Copy value types
            *self = self.deep_clone();
        }
        self
    }

    /// Create a deep clone of this node.
    /// For leaf nodes: clones all values using V::clone()
    /// For internal nodes: clones all children (incrementing their refcounts)
    fn deep_clone(&self) -> Self {
        let len = self.len();

        if self.is_leaf() {
            let mut new_node = NodePtr::new_leaf();
            // Do NOT set len yet to ensure exception safety!
            // If V::clone() panics, new_node.drop() will only drop 0 elements.

            // Copy keys (i64 is Copy, byte copy is fine)
            // SAFETY: Both src and dst are valid pointers within their respective node allocations.
            // Keys are i64 (Copy type), so byte copy is safe. len <= MAX_KEYS.
            unsafe {
                let k_src = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *const i64;
                let k_dst = new_node.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::copy_nonoverlapping(k_src, k_dst, len);
            }

            // Clone values using V::clone() - critical for non-Copy types!
            // SAFETY: v_src points to len valid V instances. v_dst points to uninitialized memory.
            // We use ptr::write to initialize each slot, and increment len after each successful
            // clone to maintain exception safety (if clone panics, only initialized values are dropped).
            unsafe {
                let v_src = self.ptr.data_ptr_mut().add(Self::values_offset()) as *const V;
                let v_dst = new_node.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
                for i in 0..len {
                    ptr::write(v_dst.add(i), (*v_src.add(i)).clone());
                    // Increment len after successful write/clone
                    // If clone panics on next iteration, this node will correctly drop 'i' elements
                    new_node.set_len(i + 1);
                }
            }

            new_node
        } else {
            let mut new_node = NodePtr::new_internal();
            new_node.set_len(len);

            // Copy keys (i64 is Copy)
            // SAFETY: Both src and dst are valid pointers within their respective node allocations.
            // Keys are i64 (Copy type), so byte copy is safe. len <= MAX_KEYS.
            unsafe {
                let k_src = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *const i64;
                let k_dst = new_node.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::copy_nonoverlapping(k_src, k_dst, len);
            }

            // Clone children (NodePtr::clone increments CompactArc refcount)
            // NodePtr::clone cannot panic, so strict exception safety sequence not needed here,
            // but setting len upfront is fine.
            // SAFETY: c_src points to len+1 valid NodePtr instances. c_dst points to uninitialized memory.
            // NodePtr::clone only does atomic operations and cannot panic.
            unsafe {
                let c_src =
                    self.ptr.data_ptr_mut().add(Self::children_offset()) as *const NodePtr<V>;
                let c_dst =
                    new_node.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
                for i in 0..=len {
                    // children count = keys count + 1
                    ptr::write(c_dst.add(i), (*c_src.add(i)).clone());
                }
            }

            new_node
        }
    }

    fn header(&self) -> &NodeHeader {
        // SAFETY: ptr points to a valid CompactArc allocation that starts with NodeHeader.
        // Use data_ptr_mut() to bypass Deref and avoid Stacked Borrows conflicts.
        unsafe { &*(self.ptr.data_ptr_mut() as *const NodeHeader) }
    }

    fn header_mut(&mut self) -> &mut NodeHeader {
        // Assumes we have unique access (called make_mut)
        // SAFETY: ptr points to a valid CompactArc allocation. Caller guarantees unique access.
        // Use data_ptr_mut() to bypass Deref and avoid Stacked Borrows conflicts:
        // going through <[u8]>::as_ptr() would create a SharedReadOnly tag that
        // prevents the &mut cast needed here.
        unsafe { &mut *(self.ptr.data_ptr_mut() as *mut NodeHeader) }
    }

    fn is_leaf(&self) -> bool {
        self.header().is_leaf == 1
    }

    fn len(&self) -> usize {
        self.header().len as usize
    }

    fn set_len(&mut self, len: usize) {
        self.header_mut().len = len as u16;
    }

    fn keys(&self) -> &[i64] {
        let len = self.len();
        // SAFETY: ptr + keys_offset points to an array of len initialized i64 keys.
        // The slice lifetime is tied to &self, ensuring validity.
        unsafe {
            let ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *const i64;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    fn values(&self) -> &[V] {
        assert!(self.is_leaf());
        let len = self.len();
        // SAFETY: This is a leaf node (asserted). ptr + values_offset points to
        // an array of len initialized V values. The slice lifetime is tied to &self.
        unsafe {
            let ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *const V;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    fn values_mut_slice(&mut self) -> &mut [V] {
        assert!(self.is_leaf());
        let len = self.len();
        // SAFETY: This is a leaf node (asserted). ptr + values_offset points to
        // an array of len initialized V values. Caller has &mut self, ensuring unique access.
        unsafe {
            let ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            std::slice::from_raw_parts_mut(ptr, len)
        }
    }

    fn children(&self) -> &[NodePtr<V>] {
        assert!(!self.is_leaf());
        let len = self.len() + 1; // Children = keys + 1
                                  // SAFETY: This is an internal node (asserted). ptr + children_offset points to
                                  // an array of len initialized NodePtr children. The slice lifetime is tied to &self.
        unsafe {
            let ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *const NodePtr<V>;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    fn children_mut_slice(&mut self) -> &mut [NodePtr<V>] {
        assert!(!self.is_leaf());
        let len = self.len() + 1;
        // SAFETY: This is an internal node (asserted). ptr + children_offset points to
        // an array of len initialized NodePtr children. Caller has &mut self, ensuring unique access.
        unsafe {
            let ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            std::slice::from_raw_parts_mut(ptr, len)
        }
    }

    fn child(&self, index: usize) -> &NodePtr<V> {
        &self.children()[index]
    }

    fn child_mut(&mut self, index: usize) -> &mut NodePtr<V> {
        &mut self.children_mut_slice()[index]
    }

    fn search(&self, key: i64) -> Result<usize, usize> {
        self.keys().binary_search(&key)
    }

    fn push_leaf(&mut self, key: i64, value: V) {
        assert!(self.is_leaf());
        let len = self.len();
        assert!(len <= MAX_KEYS); // Allow temporary overflow

        // SAFETY: This is a leaf node (asserted). len <= MAX_KEYS, so index len is within
        // the allocated array bounds (size MAX_KEYS + 1). We write to uninitialized slots.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::write(k_ptr.add(len), key);

            let v_ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            ptr::write(v_ptr.add(len), value);
        }
        self.set_len(len + 1);
    }

    fn remove_leaf(&mut self, index: usize) -> V {
        assert!(self.is_leaf());
        let len = self.len();
        assert!(index < len);

        // SAFETY: This is a leaf node (asserted). index < len, so all accesses are in bounds.
        // We read the value at index (moving it out), then shift remaining elements left.
        // The last position becomes logically invalid and is excluded by set_len.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy(k_ptr.add(index + 1), k_ptr.add(index), len - index - 1);

            let v_ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            let val = ptr::read(v_ptr.add(index));
            ptr::copy(v_ptr.add(index + 1), v_ptr.add(index), len - index - 1);

            self.set_len(len - 1);
            val
        }
    }

    fn insert_leaf(&mut self, index: usize, key: i64, value: V) {
        assert!(self.is_leaf());
        let len = self.len();
        assert!(len <= MAX_KEYS);
        assert!(
            index <= len,
            "insert_leaf: index {} > len {} for key {}",
            index,
            len,
            key
        );

        // SAFETY: This is a leaf node (asserted). len <= MAX_KEYS, so we have room for one more.
        // We shift elements at [index..len] to [index+1..len+1], then write the new key/value at index.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            let p_key = k_ptr.add(index);
            ptr::copy(p_key, p_key.add(1), len - index);
            ptr::write(p_key, key);

            let v_ptr = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            let p_val = v_ptr.add(index);
            ptr::copy(p_val, p_val.add(1), len - index);
            ptr::write(p_val, value);
        }
        self.set_len(len + 1);
    }

    fn push_internal(&mut self, key: i64, child: NodePtr<V>) {
        assert!(!self.is_leaf());
        let len = self.len();
        assert!(len <= MAX_KEYS);

        // SAFETY: This is an internal node (asserted). len <= MAX_KEYS, so indices len (for key)
        // and len+1 (for child) are within allocated bounds. We write to uninitialized slots.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::write(k_ptr.add(len), key);

            let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            ptr::write(c_ptr.add(len + 1), child);
        }
        self.set_len(len + 1);
    }

    fn split_internal(&mut self) -> (i64, NodePtr<V>) {
        let len = self.len();
        let mid = len / 2;
        let med_key = self.keys()[mid];
        let mut right = NodePtr::new_internal();
        let right_keys_count = len - mid - 1;
        let right_children_count = right_keys_count + 1;

        // SAFETY: Both self and right are internal nodes. We copy keys from indices [mid+1..len)
        // and children from indices [mid+1..len+1) of self into the start of right. The source
        // and destination do not overlap (different allocations). Keys are Copy (i64). Children
        // (NodePtr<V>) are bitwise copied - this is safe because we set self.len = mid afterwards,
        // so those slots become logically uninitialized (won't be dropped when self is dropped).
        unsafe {
            let k_src = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            let k_dst = right.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy_nonoverlapping(k_src.add(mid + 1), k_dst, right_keys_count);

            let c_src = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            let c_dst = right.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            ptr::copy_nonoverlapping(c_src.add(mid + 1), c_dst, right_children_count);
        }

        right.set_len(right_keys_count);
        self.set_len(mid);
        (med_key, right)
    }

    /// Optimized split for rightmost (sequential) inserts on internal nodes.
    /// Keeps MAX_KEYS keys in left node, moves only the last child to right.
    /// The median key (last key) goes to parent. Right node has 0 keys, 1 child.
    fn split_internal_rightmost(&mut self) -> (i64, NodePtr<V>) {
        let len = self.len();
        debug_assert!(
            len == MAX_KEYS + 1,
            "split_internal_rightmost expects overflow node"
        );

        // Median is the last key - it goes to parent
        let med_key = self.keys()[len - 1];

        let mut right = NodePtr::new_internal();

        // SAFETY: self is an internal node with MAX_KEYS + 1 keys (MAX_KEYS + 2 children).
        // We move only the last child to right. Children are moved via ptr::read then ptr::write.
        // After set_len(len-1), the source slots are logically uninitialized.
        unsafe {
            let c_src = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            let c_dst = right.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;

            // Move last child (index len, since we have len+1 children)
            ptr::write(c_dst, ptr::read(c_src.add(len)));
        }

        right.set_len(0); // 0 keys, but 1 child
        self.set_len(len - 1); // MAX_KEYS keys, MAX_KEYS + 1 children

        (med_key, right)
    }

    fn borrow_from_left(&mut self, index: usize) {
        assert!(!self.is_leaf());
        let is_left_leaf = self.child(index - 1).is_leaf();

        if is_left_leaf {
            let (key, val) = {
                let left = self.child_mut(index - 1).make_mut();
                let left_len = left.len();
                let key = left.keys()[left_len - 1];
                // SAFETY: left is a leaf node (verified by is_left_leaf). left_len > 0 because
                // we only borrow from siblings with spare keys. Index left_len - 1 is valid.
                // We move the value out via ptr::read, then set_len(left_len - 1) marks it
                // as logically removed so it won't be double-dropped.
                let val = unsafe {
                    let val_ptr = left.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
                    ptr::read(val_ptr.add(left_len - 1))
                };
                left.set_len(left_len - 1);
                (key, val)
            };

            let current = self.child_mut(index).make_mut();
            current.insert_leaf(0, key, val);

            // SAFETY: self is an internal node (asserted). index > 0 (we're borrowing from left).
            // index - 1 is a valid key index in self (parent of left and current).
            // Writing i64 which is Copy, overwriting existing separator key.
            unsafe {
                let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::write(k_ptr.add(index - 1), key);
            }
        } else {
            let (child, key) = {
                let left = self.child_mut(index - 1).make_mut();
                let left_len = left.len();
                let key = left.keys()[left_len - 1];
                // SAFETY: left is an internal node (is_left_leaf is false). left_len > 0.
                // Index left_len is valid for children array (internal nodes have len+1 children).
                // We move the child out via ptr::read, then set_len(left_len - 1) ensures it
                // won't be double-dropped.
                let child = unsafe {
                    let c_ptr =
                        left.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
                    ptr::read(c_ptr.add(left_len))
                };
                left.set_len(left_len - 1);
                (child, key)
            };

            let separator_idx = index - 1;
            let separator = self.keys()[separator_idx];

            let current = self.child_mut(index).make_mut();
            current.insert_internal_at_start(separator, child);

            // SAFETY: self is an internal node (asserted). separator_idx is valid key index.
            // Writing i64 which is Copy, overwriting existing separator key.
            unsafe {
                let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::write(k_ptr.add(separator_idx), key);
            }
        }
    }

    fn insert_internal(&mut self, index: usize, key: i64, child: NodePtr<V>) {
        assert!(!self.is_leaf());
        let len = self.len();
        assert!(len <= MAX_KEYS);

        // SAFETY: This is an internal node (asserted). len <= MAX_KEYS, so there's space.
        // index <= len. We shift keys [index..len) right by 1 to make room, then write key
        // at index. We shift children [index+1..len+1) right by 1, then write new child at
        // index+1. ptr::copy handles overlapping regions correctly. Keys are Copy (i64).
        // The new child is moved in (not cloned) via ptr::write.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy(k_ptr.add(index), k_ptr.add(index + 1), len - index);
            ptr::write(k_ptr.add(index), key);

            let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            ptr::copy(c_ptr.add(index + 1), c_ptr.add(index + 2), len - index);
            ptr::write(c_ptr.add(index + 1), child);
        }
        self.set_len(len + 1);
    }

    fn insert_internal_at_start(&mut self, key: i64, child: NodePtr<V>) {
        let len = self.len();
        // SAFETY: This is an internal node (only called from internal node contexts).
        // We shift all keys [0..len) right by 1 and write new key at index 0.
        // We shift all children [0..len+1) right by 1 and write new child at index 0.
        // ptr::copy handles overlapping regions correctly. Keys are Copy (i64).
        // The new child is moved in via ptr::write. len + 1 <= MAX_KEYS + 1 by B-tree invariants.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy(k_ptr, k_ptr.add(1), len);
            ptr::write(k_ptr, key);

            let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            ptr::copy(c_ptr, c_ptr.add(1), len + 1);
            ptr::write(c_ptr, child);
        }
        self.set_len(len + 1);
    }

    fn borrow_from_right(&mut self, index: usize) {
        assert!(!self.is_leaf());
        let is_right_leaf = self.child(index + 1).is_leaf();

        if is_right_leaf {
            let (key, val) = {
                let right = self.child_mut(index + 1).make_mut();
                let key = right.keys()[0];
                let val = right.remove_leaf(0);
                (key, val)
            };

            let current = self.child_mut(index).make_mut();
            current.push_leaf(key, val);

            let right = self.child(index + 1);
            let new_sep = right.keys()[0];
            // SAFETY: self is an internal node (asserted). index is valid key index in self
            // (it's the separator between children at index and index+1).
            // Writing i64 which is Copy, overwriting existing separator key.
            unsafe {
                let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::write(k_ptr.add(index), new_sep);
            }
        } else {
            let (child, key) = {
                let right = self.child_mut(index + 1).make_mut();
                let key = right.keys()[0];
                // SAFETY: right is an internal node (is_right_leaf is false). right.len() > 0.
                // Index 0 is valid for children array. We move the first child out via ptr::read,
                // then remove_internal_at_start() shifts remaining elements left and decrements len.
                let child = unsafe {
                    let c_ptr =
                        right.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
                    ptr::read(c_ptr)
                };
                right.remove_internal_at_start();
                (child, key)
            };

            let separator = self.keys()[index];

            let current = self.child_mut(index).make_mut();
            current.push_internal(separator, child);

            // SAFETY: self is an internal node (asserted). index is valid key index.
            // Writing i64 which is Copy, overwriting existing separator key.
            unsafe {
                let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
                ptr::write(k_ptr.add(index), key);
            }
        }
    }

    fn remove_internal_at_start(&mut self) {
        let len = self.len();
        // SAFETY: This is an internal node (only called from internal node contexts). len > 0.
        // We shift keys [1..len) left by 1 (overwriting key at index 0).
        // We shift children [1..len+1) left by 1 (overwriting child at index 0, which was
        // already moved out by caller via ptr::read). Keys are Copy (i64). Children are
        // bitwise copied (ptr::copy handles overlap correctly). The child at the end becomes
        // logically uninitialized after set_len decrements the count.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy(k_ptr.add(1), k_ptr, len - 1);

            let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            ptr::copy(c_ptr.add(1), c_ptr, len);
        }
        self.set_len(len - 1);
    }

    fn merge_with_left(&mut self, index: usize) {
        let separator = self.keys()[index - 1];

        // Make right uniquely owned so we can move data out of it
        let right_raw = self.child_mut(index) as *mut NodePtr<V>;
        // SAFETY: right_raw points to a valid NodePtr<V> in self's children array at index.
        // We need a raw pointer here to avoid borrow checker issues: we'll access left sibling
        // later, but Rust sees child_mut(index) and child_mut(index-1) as conflicting borrows.
        // The raw pointer lets us work around this while maintaining actual safety since we
        // only access right through right_raw, and later access left through child_mut.
        let right = unsafe { (*right_raw).make_mut() };
        let is_leaf = right.is_leaf();
        let r_len = right.len();

        if is_leaf {
            // Move keys and values out of right using ptr::read (no clone!)
            let mut keys_vals: Vec<(i64, V)> = Vec::with_capacity(r_len);
            // SAFETY: right is a leaf node (verified by is_leaf). We read all keys and values
            // from indices [0..r_len). Keys are Copy (i64). Values are moved via ptr::read.
            // We set_len(0) immediately after to mark them as logically removed, preventing
            // double-free when right's NodePtr is eventually dropped.
            unsafe {
                let k_ptr = right.ptr.data_ptr_mut().add(Self::keys_offset()) as *const i64;
                let v_ptr = right.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
                for i in 0..r_len {
                    let key = *k_ptr.add(i);
                    let val = ptr::read(v_ptr.add(i)); // Move, not clone!
                    keys_vals.push((key, val));
                }
            }
            // Set len=0 BEFORE any other operation to prevent double-free when right is dropped
            right.set_len(0);

            // Now we can safely mutably borrow left
            let left = self.child_mut(index - 1).make_mut();
            for (k, v) in keys_vals {
                left.push_leaf(k, v);
            }
        } else {
            // Move keys and children out of right using ptr::read (not clone!)
            // After make_mut(), we have unique ownership of the node structure,
            // so we can safely move the children out.
            let keys: Vec<i64> = right.keys().to_vec();
            // SAFETY: right is an internal node. We read all children from indices [0..r_len+1).
            // Children are moved via ptr::read.
            let children: Vec<NodePtr<V>> = unsafe {
                let c_ptr =
                    right.ptr.data_ptr_mut().add(Self::children_offset()) as *const NodePtr<V>;
                (0..=r_len).map(|i| ptr::read(c_ptr.add(i))).collect()
            };

            // CRITICAL: Set drop_count to 2 to prevent NodePtr::drop from dropping
            // the children again (they've been moved to the children Vec).
            // For internal nodes, Drop iterates 0..=len, so with len=0 it would still
            // try to drop child 0. By setting drop_count > 1, Drop thinks this isn't
            // the last reference and skips content dropping entirely.
            // SAFETY: We have unique access to right via make_mut(). Setting drop_count
            // to 2 is safe because we're about to drop this node in remove_key_and_child,
            // and we want Drop to skip the children (they've been moved out).
            unsafe {
                let header = &*(right.ptr.data_ptr_mut() as *const NodeHeader);
                header.drop_count.store(2, Ordering::Release);
            }
            right.set_len(0);

            let left = self.child_mut(index - 1).make_mut();

            // Move children to left using into_iter (no cloning needed)
            let mut children_iter = children.into_iter();
            left.push_internal(separator, children_iter.next().unwrap());
            for (key, child) in keys.into_iter().zip(children_iter) {
                left.push_internal(key, child);
            }
        }

        self.remove_key_and_child(index - 1, index);
    }

    fn merge_with_right(&mut self, index: usize) {
        self.merge_with_left(index + 1)
    }

    fn remove_key_and_child(&mut self, key_idx: usize, child_idx: usize) {
        let len = self.len();
        // SAFETY: This is an internal node (only called from internal node contexts).
        // key_idx < len and child_idx <= len (valid indices). We shift keys [key_idx+1..len)
        // left by 1 to fill the gap. For children, we first drop_in_place the child being
        // removed (it's a NodePtr that needs cleanup), then shift children [child_idx+1..len+1)
        // left by 1. ptr::copy handles overlapping regions. Keys are Copy (i64). The slots
        // at the end become logically uninitialized after set_len decrements the count.
        unsafe {
            let k_ptr = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy(
                k_ptr.add(key_idx + 1),
                k_ptr.add(key_idx),
                len - key_idx - 1,
            );

            let c_ptr = self.ptr.data_ptr_mut().add(Self::children_offset()) as *mut NodePtr<V>;
            // Drop the child being removed before overwriting it
            ptr::drop_in_place(c_ptr.add(child_idx));
            ptr::copy(
                c_ptr.add(child_idx + 1),
                c_ptr.add(child_idx),
                len - child_idx,
            );
        }
        self.set_len(len - 1);
    }

    fn split_leaf(&mut self) -> (i64, NodePtr<V>) {
        let mid = self.len() / 2;
        let right_count = self.len() - mid;

        let mut right = NodePtr::new_leaf();

        // SAFETY: Both self and right are leaf nodes. We copy keys from indices [mid..len)
        // and values from indices [mid..len) of self into the start of right. The source
        // and destination do not overlap (different allocations). Keys are Copy (i64). Values
        // are bitwise copied - this is safe because we set self.len = mid afterwards, so those
        // slots become logically uninitialized (won't be dropped when self is dropped).
        unsafe {
            let k_src = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            let k_dst = right.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;
            ptr::copy_nonoverlapping(k_src.add(mid), k_dst, right_count);

            let v_src = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            let v_dst = right.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            ptr::copy_nonoverlapping(v_src.add(mid), v_dst, right_count);
        }

        right.set_len(right_count);
        self.set_len(mid);

        let median = right.keys()[0];
        (median, right)
    }

    /// Optimized split for rightmost (sequential) inserts.
    /// Keeps MAX_KEYS in left node, moves only 1 key to right.
    /// This achieves ~100% fill factor for sequential insert workloads
    /// instead of the standard 50% from midpoint splits.
    fn split_leaf_rightmost(&mut self) -> (i64, NodePtr<V>) {
        let len = self.len();
        debug_assert!(
            len == MAX_KEYS + 1,
            "split_leaf_rightmost expects overflow node"
        );

        let mut right = NodePtr::new_leaf();

        // SAFETY: self is a leaf with MAX_KEYS + 1 elements. We move only the last
        // key/value to right. The key is Copy (i64). The value is moved via ptr::read
        // then ptr::write. After set_len(len-1), the source slot is logically uninitialized.
        unsafe {
            let k_src = self.ptr.data_ptr_mut().add(Self::keys_offset()) as *const i64;
            let k_dst = right.ptr.data_ptr_mut().add(Self::keys_offset()) as *mut i64;

            let v_src = self.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;
            let v_dst = right.ptr.data_ptr_mut().add(Self::values_offset()) as *mut V;

            // Copy last key
            ptr::write(k_dst, *k_src.add(len - 1));

            // Move last value (ptr::read moves ownership out)
            ptr::write(v_dst, ptr::read(v_src.add(len - 1)));
        }

        right.set_len(1);
        self.set_len(len - 1); // MAX_KEYS

        let median = right.keys()[0];
        (median, right)
    }
}

/// Copy-on-Write B+ tree for i64 keys
///
/// Provides lock-free reads through structural sharing.
/// Clone is O(1) - just increments root's reference count.
///
/// # Thread Safety
///
/// - **Readers**: Multiple readers can safely access the tree concurrently via cloned
///   snapshots. Each snapshot is immutable from the reader's perspective.
/// - **Writers**: Write operations (`insert`, `remove`, `get_mut`, `entry`) require
///   exclusive access (`&mut self`). External synchronization (e.g., `Mutex`, `RwLock`)
///   is required if multiple threads need write access.
/// - **Pattern**: Clone the tree to create a snapshot, then readers use the snapshot
///   while writers mutate the original. This is the standard MVCC pattern.
const MAX_TREE_DEPTH: usize = 16;

/// Stack-based path to avoid allocations
#[derive(Clone, Copy)]
pub struct NodePath {
    indices: [u8; MAX_TREE_DEPTH],
    len: u8,
}

impl Default for NodePath {
    fn default() -> Self {
        Self {
            indices: [0; MAX_TREE_DEPTH],
            len: 0,
        }
    }
}

impl NodePath {
    pub fn new() -> Self {
        Self::default()
    }

    #[cold]
    #[inline(never)]
    fn depth_overflow() -> ! {
        panic!("B-Tree depth exceeded maximum")
    }

    pub fn push(&mut self, idx: usize) {
        if (self.len as usize) < MAX_TREE_DEPTH {
            self.indices[self.len as usize] = idx as u8;
            self.len += 1;
        } else {
            Self::depth_overflow();
        }
    }

    fn get(&self, depth: usize) -> usize {
        self.indices[depth] as usize
    }

    fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len as usize).map(|i| self.indices[i] as usize)
    }
}

pub struct CowBTree<V: Clone> {
    root: Option<NodePtr<V>>,
    /// Cached maximum key in the tree. Valid if root.is_some().
    max_key: i64,
    /// Number of elements in the tree
    len: usize,
}

impl<V: Clone> Default for CowBTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone> Clone for CowBTree<V> {
    /// O(1) clone - just increments root's reference count
    #[inline]
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            max_key: self.max_key,
            len: self.len,
        }
    }
}

impl<V: Clone> CowBTree<V> {
    #[inline]
    pub fn new() -> Self {
        assert!(
            mem::align_of::<V>() <= 8,
            "CowBTree value type alignment must be <= 8"
        );
        Self {
            root: None,
            max_key: 0,
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Get a value by key. Lock-free, O(log n).
    #[inline]
    pub fn get(&self, key: i64) -> Option<&V> {
        let mut node = self.root.as_ref()?;
        loop {
            match node.search(key) {
                Ok(i) => {
                    if node.is_leaf() {
                        return Some(&node.values()[i]);
                    }
                    node = node.child(i + 1);
                }
                Err(i) => {
                    if node.is_leaf() {
                        return None;
                    }
                    node = node.child(i);
                }
            }
        }
    }

    /// Get a mutable reference to a value. Triggers COW if needed.
    #[inline]
    pub fn get_mut(&mut self, key: i64) -> Option<&mut V> {
        let (path, leaf_idx) = self.search_path(key);
        let leaf_idx = leaf_idx.ok()?;

        self.get_mut_with_path(key, &path, leaf_idx)
    }

    fn get_mut_with_path(&mut self, _key: i64, path: &NodePath, leaf_idx: usize) -> Option<&mut V> {
        let root = self.root.as_mut()?;
        let mut node = root.make_mut();

        for idx in path.iter() {
            let child = node.child_mut(idx);
            node = child.make_mut();
        }

        if leaf_idx < node.len() {
            Some(&mut node.values_mut_slice()[leaf_idx])
        } else {
            None
        }
    }

    /// Check if key exists. Lock-free, O(log n).
    #[inline]
    pub fn contains_key(&self, key: i64) -> bool {
        self.get(key).is_some()
    }

    /// Insert a key-value pair. Returns old value if key existed.
    pub fn insert(&mut self, key: i64, value: V) -> Option<V> {
        if self.root.is_none() {
            let mut node = NodePtr::new_leaf();
            node.push_leaf(key, value);
            self.root = Some(node);
            self.max_key = key;
            self.len = 1;
            return None;
        }

        // Fast path for sequential inserts: if key > max key, append to rightmost leaf
        if self.is_key_greater_than_max(key) {
            let root = self.root.as_mut().unwrap();
            let result = Self::insert_rightmost(root, key, value);
            self.max_key = key;

            return match result {
                InsertResult::Done(old) => {
                    if old.is_none() {
                        self.len += 1;
                    }
                    old
                }
                InsertResult::Split(median, right) => {
                    let old_root = self.root.take().unwrap();
                    let mut new_root = NodePtr::new_internal();

                    // SAFETY: new_root is a freshly created internal node with len=0.
                    // We write old_root to children[0]. Internal nodes have len+1 children,
                    // so with len=0 we have space for 1 child at index 0. old_root is moved
                    // (not cloned) into the slot. push_internal will add children[1] and set len=1.
                    unsafe {
                        let c_ptr = new_root
                            .ptr
                            .data_ptr_mut()
                            .add(NodePtr::<V>::children_offset())
                            as *mut NodePtr<V>;
                        ptr::write(c_ptr, old_root);
                    }

                    new_root.push_internal(median, right);
                    self.root = Some(new_root);
                    self.len += 1;
                    None
                }
            };
        }

        let root = self.root.as_mut().unwrap();
        let result = Self::insert_recursive(root, key, value);

        if key > self.max_key {
            self.max_key = key;
        }

        match result {
            InsertResult::Done(old) => {
                if old.is_none() {
                    self.len += 1;
                }
                old
            }
            InsertResult::Split(median, right) => {
                let old_root = self.root.take().unwrap();
                let mut new_root = NodePtr::new_internal();
                // SAFETY: new_root is a freshly created internal node with len=0.
                // We write old_root to children[0]. Internal nodes have len+1 children,
                // so with len=0 we have space for 1 child at index 0. old_root is moved
                // (not cloned) into the slot. push_internal will add children[1] and set len=1.
                unsafe {
                    let c_ptr = new_root
                        .ptr
                        .data_ptr_mut()
                        .add(NodePtr::<V>::children_offset())
                        as *mut NodePtr<V>;
                    ptr::write(c_ptr, old_root);
                }
                new_root.push_internal(median, right);
                self.root = Some(new_root);
                self.len += 1;
                None
            }
        }
    }

    /// Check if key is greater than maximum key in tree (O(1) with caching)
    #[inline]
    fn is_key_greater_than_max(&self, key: i64) -> bool {
        self.root.is_some() && key > self.max_key
    }

    /// Fast path: insert into rightmost leaf (for sequential inserts)
    fn insert_rightmost(node: &mut NodePtr<V>, key: i64, value: V) -> InsertResult<V> {
        let (res, _) = Self::insert_rightmost_return_ptr(node, key, value);
        res
    }

    fn insert_rightmost_return_ptr(
        node: &mut NodePtr<V>,
        key: i64,
        value: V,
    ) -> (InsertResult<V>, *mut V) {
        let node = node.make_mut();

        if node.is_leaf() {
            node.push_leaf(key, value);

            // SAFETY: node is a leaf (verified above). We just pushed a value, so len >= 1.
            // We compute a pointer to the newly inserted value (at index len - 1).
            // If the node overflows (len > MAX_KEYS), we use rightmost split which moves
            // only the last element to the new right node, so the pointer is at right[0].
            // All pointer arithmetic stays within allocated bounds.
            unsafe {
                let len = node.len();
                let v_ptr = node.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
                let ptr = v_ptr.add(len - 1);

                if node.len() > MAX_KEYS {
                    // Use rightmost split: keeps MAX_KEYS in left, moves 1 to right
                    let (median, right) = node.split_leaf_rightmost();
                    // After rightmost split, the inserted value is at right[0]
                    let v_ptr_new =
                        right.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
                    (InsertResult::Split(median, right), v_ptr_new)
                } else {
                    (InsertResult::Done(None), ptr)
                }
            }
        } else {
            let last_idx = node.len();
            let child = node.child_mut(last_idx);
            let (result, ptr) = Self::insert_rightmost_return_ptr(child, key, value);

            match result {
                InsertResult::Done(old) => (InsertResult::Done(old), ptr),
                InsertResult::Split(median, right) => {
                    node.push_internal(median, right);

                    if node.len() > MAX_KEYS {
                        // Use rightmost split for internal nodes too
                        let (m, r) = node.split_internal_rightmost();
                        (InsertResult::Split(m, r), ptr)
                    } else {
                        (InsertResult::Done(None), ptr)
                    }
                }
            }
        }
    }

    fn insert_recursive(node: &mut NodePtr<V>, key: i64, value: V) -> InsertResult<V> {
        let node = node.make_mut();

        if node.is_leaf() {
            match node.search(key) {
                // SAFETY: node is a leaf (verified above). search returned Ok(i), meaning
                // key exists at index i (where i < node.len()). We read the old value via
                // ptr::read and write the new value via ptr::write. This is a replacement
                // of an existing value, so no len change is needed.
                Ok(i) => unsafe {
                    let v_ptr =
                        node.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
                    let old = ptr::read(v_ptr.add(i));
                    ptr::write(v_ptr.add(i), value);
                    InsertResult::Done(Some(old))
                },
                Err(i) => {
                    node.insert_leaf(i, key, value);

                    if node.len() > MAX_KEYS {
                        // Optimization: if we inserted at the end, use rightmost split
                        // This handles interleaved sequential inserts that miss the global fast path
                        let (median, right) = if i == node.len() - 1 {
                            node.split_leaf_rightmost()
                        } else {
                            node.split_leaf()
                        };
                        InsertResult::Split(median, right)
                    } else {
                        InsertResult::Done(None)
                    }
                }
            }
        } else {
            let i = match node.search(key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };

            let result = Self::insert_recursive(node.child_mut(i), key, value);

            match result {
                InsertResult::Done(old) => InsertResult::Done(old),
                InsertResult::Split(median, right) => {
                    node.insert_internal(i, median, right);

                    if node.len() > MAX_KEYS {
                        // Optimization: if we inserted at the end, use rightmost split
                        let (m, r) = if i == node.len() - 1 {
                            node.split_internal_rightmost()
                        } else {
                            node.split_internal()
                        };
                        InsertResult::Split(m, r)
                    } else {
                        InsertResult::Done(None)
                    }
                }
            }
        }
    }

    /// Remove a key. Returns the value if it existed.
    pub fn remove(&mut self, key: i64) -> Option<V> {
        let root = self.root.as_mut()?;
        let result = Self::remove_recursive(root, key);

        if result.is_some() {
            self.len -= 1;

            if self.len == 0 {
                self.root = None;
                self.max_key = 0;
            } else if self.max_key == key {
                self.refresh_max_key();
            }

            if let Some(ref mut root) = self.root {
                let root = root.make_mut();
                if !root.is_leaf() && root.len() == 0 {
                    let child_node = root.child_mut(0).clone();
                    self.root = Some(child_node);
                } else if root.is_leaf() && root.len() == 0 {
                    self.root = None;
                    self.max_key = 0;
                }
            }
        }

        result
    }

    /// Search and return path to the leaf.
    /// Returns (path_indices, leaf_search_result)
    /// path_indices: indices of children taken to reach the leaf
    fn search_path(&self, key: i64) -> (NodePath, Result<usize, usize>) {
        let mut path = NodePath::new();
        if self.root.is_none() {
            return (path, Err(0));
        }

        let mut node = self.root.as_ref().unwrap();
        loop {
            match node.search(key) {
                Ok(i) => {
                    if node.is_leaf() {
                        return (path, Ok(i));
                    }
                    path.push(i + 1);
                    node = node.child(i + 1);
                }
                Err(i) => {
                    if node.is_leaf() {
                        return (path, Err(i));
                    }
                    path.push(i);
                    node = node.child(i);
                }
            }
        }
    }

    /// Insert using pre-computed path and leaf index (avoids redundant search).
    /// `leaf_idx` is the index in the leaf where the key should be inserted.
    fn insert_with_path(
        node_ptr: &mut NodePtr<V>,
        key: i64,
        value: V,
        path: &NodePath,
        depth: usize,
        leaf_idx: usize,
    ) -> (InsertResult<V>, *mut V) {
        let node = node_ptr.make_mut();

        if node.is_leaf() {
            // Use pre-computed leaf_idx directly - no search needed
            let i = leaf_idx;
            node.insert_leaf(i, key, value);

            if node.len() > MAX_KEYS {
                let (median, right_node) = node.split_leaf();
                // Must match split_leaf's mid calculation: self.len() / 2
                // Before split, len was MAX_KEYS + 1, so mid = (MAX_KEYS + 1) / 2 = MAX_KEYS.div_ceil(2)
                let mid = MAX_KEYS.div_ceil(2);

                let ptr = if i < mid {
                    // SAFETY: After split, i < mid means the inserted value stayed in node.
                    // node is a valid leaf with i < node.len() after the split. We compute a
                    // pointer to the inserted value at index i.
                    unsafe {
                        let v_ptr =
                            node.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
                        v_ptr.add(i)
                    }
                } else {
                    let right_idx = i - mid;
                    // SAFETY: After split, i >= mid means the inserted value moved to right_node.
                    // right_node is a valid leaf with right_idx < right_node.len() after the split.
                    // We compute a pointer to the inserted value at index right_idx.
                    unsafe {
                        let v_ptr = right_node
                            .ptr
                            .data_ptr_mut()
                            .add(NodePtr::<V>::values_offset())
                            as *mut V;
                        v_ptr.add(right_idx)
                    }
                };

                (InsertResult::Split(median, right_node), ptr)
            } else {
                // SAFETY: node is a leaf, we just inserted at index i (where i < node.len()).
                // We compute a pointer to the newly inserted value.
                unsafe {
                    let v_ptr =
                        node.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
                    let ptr = v_ptr.add(i);
                    (InsertResult::Done(None), ptr)
                }
            }
        } else {
            let i = path.get(depth);

            let (result, ptr) =
                Self::insert_with_path(node.child_mut(i), key, value, path, depth + 1, leaf_idx);

            match result {
                InsertResult::Done(old) => (InsertResult::Done(old), ptr),
                InsertResult::Split(median, right) => {
                    node.insert_internal(i, median, right);

                    if node.len() > MAX_KEYS {
                        // Optimization: if we inserted at the end, use rightmost split
                        let (m, r) = if i == node.len() - 1 {
                            node.split_internal_rightmost()
                        } else {
                            node.split_internal()
                        };
                        (InsertResult::Split(m, r), ptr)
                    } else {
                        (InsertResult::Done(None), ptr)
                    }
                }
            }
        }
    }

    fn refresh_max_key(&mut self) {
        if let Some(root) = &self.root {
            let mut node = root;
            loop {
                if node.is_leaf() {
                    self.max_key = node.keys().last().copied().unwrap_or(0);
                    break;
                }
                node = node.children().last().unwrap();
            }
        } else {
            self.max_key = 0;
        }
    }

    fn remove_recursive(node: &mut NodePtr<V>, key: i64) -> Option<V> {
        let node = node.make_mut();

        if node.is_leaf() {
            match node.search(key) {
                Ok(i) => Some(node.remove_leaf(i)),
                Err(_) => None,
            }
        } else {
            let i = match node.search(key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };

            if node.child(i).len() <= MIN_KEYS {
                Self::ensure_child_can_lose_key(node, i);
            }

            let new_i = match node.search(key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };

            let i = new_i.min(node.len()); // Children len is len+1. Max index len.
            Self::remove_recursive(node.child_mut(i), key)
        }
    }

    fn ensure_child_can_lose_key(node: &mut NodePtr<V>, i: usize) {
        let can_borrow_left = i > 0 && node.child(i - 1).len() > MIN_KEYS;
        let can_borrow_right = i < node.len() && node.child(i + 1).len() > MIN_KEYS;

        if can_borrow_left {
            node.borrow_from_left(i);
        } else if can_borrow_right {
            node.borrow_from_right(i);
        } else if i > 0 {
            node.merge_with_left(i);
        } else if i < node.len() {
            node.merge_with_right(i);
        }
    }

    /// Iterate over chunks of keys and values (O(1) amortized traversal)
    /// Yields `(&[i64], &[V])` slices directly from leaf nodes.
    pub fn iter_chunks(&self) -> impl Iterator<Item = (&[i64], &[V])> {
        CowBTreeChunkIter::new(self.root.as_ref())
    }

    /// Iterate over all key-value pairs in sorted order
    pub fn iter(&self) -> impl Iterator<Item = (&i64, &V)> {
        self.iter_chunks()
            .flat_map(|(keys, values)| keys.iter().zip(values.iter()))
    }

    /// Iterate over keys in sorted order
    pub fn keys(&self) -> impl Iterator<Item = i64> + '_ {
        self.iter().map(|(k, _)| *k)
    }

    /// Iterate over values in sorted order
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    /// Yields chunks of keys and values within the range.
    /// Each chunk is a slice from a single leaf node.
    pub fn range_chunks<R>(&self, range: R) -> impl Iterator<Item = (&[i64], &[V])>
    where
        R: std::ops::RangeBounds<i64>,
    {
        CowBTreeRangeChunkIter::new(self.root.as_ref(), range)
    }

    /// Iterator over a sub-range of elements in the B-tree.
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = (&i64, &V)>
    where
        R: std::ops::RangeBounds<i64>,
    {
        self.range_chunks(range)
            .flat_map(|(keys, values)| keys.iter().zip(values.iter()))
    }

    /// Iterate over chunks in reverse order (rightmost leaf to leftmost)
    pub fn iter_rev_chunks(&self) -> impl Iterator<Item = (&[i64], &[V])> {
        CowBTreeRevChunkIter::new(self.root.as_ref())
    }

    /// Iterate over all key-value pairs in reverse sorted order (largest to smallest)
    pub fn iter_rev(&self) -> impl Iterator<Item = (&i64, &V)> {
        self.iter_rev_chunks()
            .flat_map(|(keys, values)| keys.iter().zip(values.iter()).rev())
    }

    /// Yields chunks within the range in reverse order (rightmost matching leaf first).
    /// Each chunk is a slice from a single leaf node, keys in ascending order within the chunk.
    pub fn range_rev_chunks<R>(&self, range: R) -> impl Iterator<Item = (&[i64], &[V])>
    where
        R: std::ops::RangeBounds<i64>,
    {
        CowBTreeRevRangeChunkIter::new(self.root.as_ref(), range)
    }

    /// Iterator over a sub-range in reverse order (largest to smallest within range).
    pub fn range_rev<R>(&self, range: R) -> impl Iterator<Item = (&i64, &V)>
    where
        R: std::ops::RangeBounds<i64>,
    {
        self.range_rev_chunks(range)
            .flat_map(|(keys, values)| keys.iter().zip(values.iter()).rev())
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.root = None;
        self.max_key = 0;
        self.len = 0;
    }

    /// Returns the cached maximum key, or None if the tree is empty.
    #[inline]
    pub fn max_key(&self) -> Option<i64> {
        if self.root.is_some() {
            Some(self.max_key)
        } else {
            None
        }
    }

    fn insert_rightmost_entry(&mut self, key: i64, value: V) -> *mut V {
        let root = self.root.as_mut().unwrap();
        let (result, ptr) = Self::insert_rightmost_return_ptr(root, key, value);
        self.max_key = key;

        match result {
            InsertResult::Done(old) => {
                if old.is_none() {
                    self.len += 1;
                }
            }
            InsertResult::Split(median, right) => {
                let old_root = self.root.take().unwrap();
                let mut new_root = NodePtr::new_internal();
                // SAFETY: new_root is a freshly created internal node with len=0.
                // We write old_root to children[0]. Internal nodes have len+1 children,
                // so with len=0 we have space for 1 child at index 0. old_root is moved
                // (not cloned) into the slot. push_internal will add children[1] and set len=1.
                unsafe {
                    let c_ptr = new_root
                        .ptr
                        .data_ptr_mut()
                        .add(NodePtr::<V>::children_offset())
                        as *mut NodePtr<V>;
                    ptr::write(c_ptr, old_root);
                }
                new_root.push_internal(median, right);
                self.root = Some(new_root);
                self.len += 1;
            }
        }

        ptr
    }

    /// Entry API for in-place updates.
    /// Optimized: single traversal, O(1) get().
    /// - entry(): 1 traversal
    /// - OccupiedEntry::get(): O(1) via cached pointer
    /// - OccupiedEntry::get_mut()/insert(): 1 traversal with COW
    pub fn entry(&mut self, key: i64) -> Entry<'_, V> {
        // Fast path for sequential append
        if self.is_key_greater_than_max(key) {
            return Entry::Vacant(VacantEntry {
                tree: self,
                key,
                path: NodePath::new(), // Dummy, not used for rightmost
                leaf_idx: 0,           // Dummy, not used for rightmost
                is_rightmost: true,
            });
        }

        let (path, leaf_result) = self.search_path(key);
        match leaf_result {
            Ok(idx) => Entry::Occupied(OccupiedEntry {
                tree: self,
                key,
                path,
                leaf_idx: idx,
            }),
            Err(idx) => Entry::Vacant(VacantEntry {
                tree: self,
                key,
                path,
                leaf_idx: idx,
                is_rightmost: false,
            }),
        }
    }

    fn insert_using_path(
        &mut self,
        key: i64,
        value: V,
        path: &NodePath,
        leaf_idx: usize,
    ) -> *mut V {
        if self.root.is_none() {
            self.insert(key, value);
            return self.get_mut(key).unwrap() as *mut V;
        }

        let root = self.root.as_mut().unwrap();
        let (result, ptr) = Self::insert_with_path(root, key, value, path, 0, leaf_idx);

        if key > self.max_key {
            self.max_key = key;
        }

        match result {
            InsertResult::Done(old) => {
                if old.is_none() {
                    self.len += 1;
                }
            }
            InsertResult::Split(median, right) => {
                let old_root = self.root.take().unwrap();
                let mut new_root = NodePtr::new_internal();
                // SAFETY: new_root is a freshly created internal node with len=0.
                // We write old_root to children[0]. Internal nodes have len+1 children,
                // so with len=0 we have space for 1 child at index 0. old_root is moved
                // (not cloned) into the slot. push_internal will add children[1] and set len=1.
                unsafe {
                    let c_ptr = new_root
                        .ptr
                        .data_ptr_mut()
                        .add(NodePtr::<V>::children_offset())
                        as *mut NodePtr<V>;
                    ptr::write(c_ptr, old_root);
                }
                new_root.push_internal(median, right);
                self.root = Some(new_root);
                self.len += 1;
            }
        }

        ptr
    }
}

/// Entry API for CowBTree
pub enum Entry<'a, V: Clone> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}

impl<'a, V: Clone> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }

    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Entry::Occupied(mut entry) => {
                f(entry.get_mut());
                Entry::Occupied(entry)
            }
            Entry::Vacant(entry) => Entry::Vacant(entry),
        }
    }

    pub fn key(&self) -> i64 {
        match self {
            Entry::Occupied(entry) => entry.key(),
            Entry::Vacant(entry) => entry.key(),
        }
    }
}

/// An occupied entry in the CowBTree
pub struct OccupiedEntry<'a, V: Clone> {
    tree: &'a mut CowBTree<V>,
    key: i64,
    path: NodePath,
    leaf_idx: usize,
}

impl<'a, V: Clone> OccupiedEntry<'a, V> {
    #[inline]
    pub fn key(&self) -> i64 {
        self.key
    }

    /// O(log n) - traverses the cached path to reach the leaf
    #[inline]
    pub fn get(&self) -> &V {
        let mut node = self.tree.root.as_ref().unwrap();
        for idx in self.path.iter() {
            node = node.child(idx);
        }
        &node.values()[self.leaf_idx]
    }

    pub fn get_mut(&mut self) -> &mut V {
        self.tree
            .get_mut_with_path(self.key, &self.path, self.leaf_idx)
            .unwrap()
    }

    pub fn into_mut(self) -> &'a mut V {
        self.tree
            .get_mut_with_path(self.key, &self.path, self.leaf_idx)
            .unwrap()
    }

    pub fn insert(&mut self, value: V) -> V {
        let node = self.tree.root.as_mut().unwrap();
        let mut node = node.make_mut();

        for idx in self.path.iter() {
            let child = node.child_mut(idx);
            node = child.make_mut();
        }

        // SAFETY: node is a leaf (we followed the path to a leaf). self.leaf_idx is the
        // index where the key was found during entry lookup, so it's valid (< node.len()).
        // We read the old value via ptr::read and write the new value via ptr::write.
        // This is a replacement of an existing value, so no len change is needed.
        unsafe {
            let v_ptr = node.ptr.data_ptr_mut().add(NodePtr::<V>::values_offset()) as *mut V;
            let ptr = v_ptr.add(self.leaf_idx);
            let old = ptr::read(ptr);
            ptr::write(ptr, value);
            old
        }
    }
}

/// A vacant entry in the CowBTree
pub struct VacantEntry<'a, V: Clone> {
    tree: &'a mut CowBTree<V>,
    key: i64,
    path: NodePath,
    /// Index in the leaf node where the key should be inserted
    leaf_idx: usize,
    /// Optimization: true if this entry represents a sequential insert (key > max_key)
    is_rightmost: bool,
}

impl<'a, V: Clone> VacantEntry<'a, V> {
    #[inline]
    pub fn key(&self) -> i64 {
        self.key
    }

    #[inline]
    pub fn insert(self, value: V) -> &'a mut V {
        if self.is_rightmost {
            let ptr = self.tree.insert_rightmost_entry(self.key, value);
            // SAFETY: insert_rightmost_entry returns a valid pointer to the newly inserted
            // value. The pointer remains valid for the lifetime 'a because we have exclusive
            // access to the tree (&'a mut). The insert functions guarantee the pointer points
            // to initialized, properly aligned memory within a leaf node.
            unsafe { &mut *ptr }
        } else {
            let ptr = self
                .tree
                .insert_using_path(self.key, value, &self.path, self.leaf_idx);
            // SAFETY: insert_using_path returns a valid pointer to the newly inserted value.
            // The pointer remains valid for the lifetime 'a because we have exclusive access
            // to the tree (&'a mut). The insert functions guarantee the pointer points to
            // initialized, properly aligned memory within a leaf node.
            unsafe { &mut *ptr }
        }
    }
}

enum InsertResult<V: Clone> {
    Done(Option<V>),
    Split(i64, NodePtr<V>),
}

/// Iterator over chunks of a CowBTree (leaf slices)
struct CowBTreeChunkIter<'a, V: Clone> {
    /// Stack of (node, next_child_index) for traversal
    /// Only holds internal nodes.
    stack: Vec<(&'a NodePtr<V>, usize)>,
    /// Current leaf node being yielded (if any)
    current_leaf: Option<&'a NodePtr<V>>,
}

impl<'a, V: Clone> CowBTreeChunkIter<'a, V> {
    fn new(root: Option<&'a NodePtr<V>>) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            current_leaf: None,
        };
        if let Some(root) = root {
            iter.descend_to_leftmost(root);
        }
        iter
    }

    /// Descend to the leftmost leaf, pushing internal nodes onto the stack
    fn descend_to_leftmost(&mut self, mut node: &'a NodePtr<V>) {
        while !node.is_leaf() {
            self.stack.push((node, 1));
            node = node.child(0);
        }
        self.current_leaf = Some(node);
    }
}

impl<'a, V: Clone> Iterator for CowBTreeChunkIter<'a, V> {
    type Item = (&'a [i64], &'a [V]);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf) = self.current_leaf.take() {
            return Some((leaf.keys(), leaf.values()));
        }

        loop {
            let (node, idx) = self.stack.last_mut()?;

            if *idx < node.len() + 1 {
                let child_idx = *idx;
                *idx += 1;
                let child = node.child(child_idx);
                self.descend_to_leftmost(child);

                if let Some(leaf) = self.current_leaf.take() {
                    return Some((leaf.keys(), leaf.values()));
                }
            } else {
                self.stack.pop();
            }
        }
    }
}

/// Range iterator over a CowBTree yielding chunks
/// Optimized: Seeks directly to start bound and yields slices
struct CowBTreeRangeChunkIter<'a, V: Clone, R> {
    stack: Vec<(&'a NodePtr<V>, usize)>,
    range: R,
    current_leaf: Option<&'a NodePtr<V>>,
    current_idx: usize,
    finished: bool,
}

impl<'a, V: Clone, R: std::ops::RangeBounds<i64>> CowBTreeRangeChunkIter<'a, V, R> {
    fn new(root: Option<&'a NodePtr<V>>, range: R) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            range,
            current_leaf: None,
            current_idx: 0,
            finished: false,
        };
        if let Some(root) = root {
            iter.seek_to_start(root);
        } else {
            iter.finished = true;
        }
        iter
    }

    fn seek_to_start(&mut self, mut node: &'a NodePtr<V>) {
        let start_key = match self.range.start_bound() {
            Bound::Included(&k) => Some(k),
            Bound::Excluded(&k) => Some(k),
            Bound::Unbounded => None,
        };

        loop {
            if node.is_leaf() {
                let keys = node.keys();
                let mut idx = if let Some(k) = start_key {
                    match keys.binary_search(&k) {
                        Ok(i) => i,
                        Err(i) => i,
                    }
                } else {
                    0
                };

                if let Bound::Excluded(&k) = self.range.start_bound() {
                    if idx < keys.len() && keys[idx] == k {
                        idx += 1;
                    }
                }

                self.current_leaf = Some(node);
                self.current_idx = idx;
                break;
            } else {
                let idx = if let Some(k) = start_key {
                    match node.search(k) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    }
                } else {
                    0
                };

                self.stack.push((node, idx + 1));
                node = node.child(idx);
            }
        }
    }
}

impl<'a, V: Clone, R: std::ops::RangeBounds<i64>> Iterator for CowBTreeRangeChunkIter<'a, V, R> {
    type Item = (&'a [i64], &'a [V]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(leaf) = self.current_leaf {
                let keys = leaf.keys();
                let values = leaf.values();
                if self.current_idx < keys.len() {
                    let start = self.current_idx;
                    let end = match self.range.end_bound() {
                        Bound::Unbounded => keys.len(),
                        Bound::Included(&k) => {
                            if keys.last().unwrap() <= &k {
                                keys.len()
                            } else {
                                let pos = keys[start..].partition_point(|&x| x <= k);
                                self.finished = true;
                                start + pos
                            }
                        }
                        Bound::Excluded(&k) => {
                            if keys.last().unwrap() < &k {
                                keys.len()
                            } else {
                                let pos = keys[start..].partition_point(|&x| x < k);
                                self.finished = true;
                                start + pos
                            }
                        }
                    };

                    if start >= end {
                        self.finished = true;
                        self.current_leaf = None;
                        return None;
                    }

                    self.current_idx = end;
                    let result = (&keys[start..end], &values[start..end]);

                    if end == keys.len() && !self.finished {
                        self.current_leaf = None;
                    } else {
                        self.finished = true;
                        self.current_leaf = None;
                    }

                    return Some(result);
                } else {
                    self.current_leaf = None;
                }
            }

            if self.finished {
                return None;
            }

            if let Some((node, idx)) = self.stack.last_mut() {
                if *idx < node.len() + 1 {
                    let child_idx = *idx;
                    *idx += 1;
                    let mut child = node.child(child_idx);

                    loop {
                        if child.is_leaf() {
                            self.current_leaf = Some(child);
                            self.current_idx = 0;
                            break;
                        } else {
                            self.stack.push((child, 1));
                            child = child.child(0);
                        }
                    }
                } else {
                    self.stack.pop();
                    if self.stack.is_empty() {
                        self.finished = true;
                        return None;
                    }
                }
            } else {
                self.finished = true;
                return None;
            }
        }
    }
}

impl<V: Clone + std::fmt::Debug> std::fmt::Debug for CowBTree<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Reverse iterator over a CowBTree yielding chunks from rightmost leaf to leftmost
struct CowBTreeRevChunkIter<'a, V: Clone> {
    /// Stack of (node, next_child_plus_one) for reverse traversal.
    /// next_child_plus_one == 0 means no more children to visit at this level.
    stack: Vec<(&'a NodePtr<V>, usize)>,
    /// Current leaf node being yielded
    current_leaf: Option<&'a NodePtr<V>>,
}

impl<'a, V: Clone> CowBTreeRevChunkIter<'a, V> {
    fn new(root: Option<&'a NodePtr<V>>) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            current_leaf: None,
        };
        if let Some(root) = root {
            iter.descend_to_rightmost(root);
        }
        iter
    }

    /// Descend to the rightmost leaf, pushing internal nodes onto the stack
    fn descend_to_rightmost(&mut self, mut node: &'a NodePtr<V>) {
        while !node.is_leaf() {
            let last_child = node.len(); // children indices: 0..=len
                                         // After visiting child(last_child), next to visit going left is child(last_child-1)
                                         // Store last_child as next_child_plus_one
            self.stack.push((node, last_child));
            node = node.child(last_child);
        }
        self.current_leaf = Some(node);
    }
}

impl<'a, V: Clone> Iterator for CowBTreeRevChunkIter<'a, V> {
    type Item = (&'a [i64], &'a [V]);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf) = self.current_leaf.take() {
            return Some((leaf.keys(), leaf.values()));
        }

        loop {
            let (node, next_plus_one) = self.stack.last_mut()?;

            if *next_plus_one > 0 {
                let child_idx = *next_plus_one - 1;
                *next_plus_one = child_idx;
                let child = node.child(child_idx);
                self.descend_to_rightmost(child);

                if let Some(leaf) = self.current_leaf.take() {
                    return Some((leaf.keys(), leaf.values()));
                }
            } else {
                self.stack.pop();
            }
        }
    }
}

/// Reverse range iterator over a CowBTree yielding chunks from end bound to start bound.
/// Each chunk contains keys in ascending order; the consumer should reverse within each chunk.
struct CowBTreeRevRangeChunkIter<'a, V: Clone, R> {
    stack: Vec<(&'a NodePtr<V>, usize)>,
    range: R,
    current_leaf: Option<&'a NodePtr<V>>,
    /// End index (exclusive) within current leaf
    current_end_idx: usize,
    finished: bool,
}

impl<'a, V: Clone, R: std::ops::RangeBounds<i64>> CowBTreeRevRangeChunkIter<'a, V, R> {
    fn new(root: Option<&'a NodePtr<V>>, range: R) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            range,
            current_leaf: None,
            current_end_idx: 0,
            finished: false,
        };
        if let Some(root) = root {
            iter.seek_to_end(root);
        } else {
            iter.finished = true;
        }
        iter
    }

    /// Descend to the rightmost leaf, setting current_end_idx to the full leaf length
    fn descend_to_rightmost(&mut self, mut node: &'a NodePtr<V>) {
        while !node.is_leaf() {
            let last_child = node.len();
            self.stack.push((node, last_child));
            node = node.child(last_child);
        }
        self.current_leaf = Some(node);
        self.current_end_idx = node.len();
    }

    /// Seek to the end bound of the range (the starting point for reverse iteration)
    fn seek_to_end(&mut self, mut node: &'a NodePtr<V>) {
        let end_key = match self.range.end_bound() {
            Bound::Included(&k) | Bound::Excluded(&k) => Some(k),
            Bound::Unbounded => None,
        };

        if end_key.is_none() {
            // Unbounded upper: start from rightmost leaf
            self.descend_to_rightmost(node);
            return;
        }

        let k = end_key.unwrap();

        loop {
            if node.is_leaf() {
                let keys = node.keys();
                let idx = match keys.binary_search(&k) {
                    Ok(i) => match self.range.end_bound() {
                        Bound::Included(_) => i + 1,
                        _ => i,
                    },
                    Err(i) => i,
                };

                if idx > 0 {
                    self.current_leaf = Some(node);
                    self.current_end_idx = idx;
                }
                // If idx == 0, no valid entries in this leaf for our range.
                // The first next() call will navigate to the previous leaf.
                break;
            } else {
                let child_idx = match node.search(k) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                // Store child_idx as next_child_plus_one: children before child_idx
                self.stack.push((node, child_idx));
                node = node.child(child_idx);
            }
        }
    }
}

impl<'a, V: Clone, R: std::ops::RangeBounds<i64>> Iterator for CowBTreeRevRangeChunkIter<'a, V, R> {
    type Item = (&'a [i64], &'a [V]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(leaf) = self.current_leaf {
                let keys = leaf.keys();
                let values = leaf.values();
                let end = self.current_end_idx;

                // Check start bound (lower bound) - trim from the left
                let start = if end == 0 {
                    end // Empty chunk, will be skipped
                } else {
                    match self.range.start_bound() {
                        Bound::Unbounded => 0,
                        Bound::Included(&k) => {
                            if keys[0] >= k {
                                0 // Entire chunk is within range
                            } else {
                                self.finished = true;
                                keys[..end].partition_point(|&x| x < k)
                            }
                        }
                        Bound::Excluded(&k) => {
                            if keys[0] > k {
                                0
                            } else {
                                self.finished = true;
                                keys[..end].partition_point(|&x| x <= k)
                            }
                        }
                    }
                };

                self.current_leaf = None;

                if start < end {
                    return Some((&keys[start..end], &values[start..end]));
                }

                if self.finished {
                    return None;
                }
            }

            // Navigate to previous leaf
            let (node, next_plus_one) = self.stack.last_mut()?;

            if *next_plus_one > 0 {
                let child_idx = *next_plus_one - 1;
                *next_plus_one = child_idx;
                let child = node.child(child_idx);
                self.descend_to_rightmost(child);
            } else {
                self.stack.pop();
            }
        }
    }
}

include!("cow_btree/tests.rs");
