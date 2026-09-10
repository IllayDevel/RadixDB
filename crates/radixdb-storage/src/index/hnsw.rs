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

//! HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search
//!
//! Provides O(log N) vector similarity search with configurable recall/speed tradeoff.
//!
//! # Parameters
//! - `m`: Max connections per node per layer (default: 16)
//! - `ef_construction`: Beam width during build (default: 200, higher = better recall)
//! - `ef_search`: Beam width during search (default: 200, higher = better recall)

use parking_lot::RwLock;
use std::collections::BinaryHeap;

use crate::expression::Expression;
use crate::traits::Index;
use radixdb_core::{DataType, Error, IndexEntry, IndexType, Operator, Result, RowIdVec, Value};
use radixdb_core::{I64Map, I64Set};

mod distance;

use distance::{
    as_f32_slice, bytes_to_f32_vec, cosine_distance_f32, ip_distance_f32, l2_distance_sq_f32,
    prefetch_read,
};

const HNSW_GRAPH_HEADER_LEN: usize = 25;
const HNSW_MAX_GRAPH_BYTES: usize = 1 << 30;
const HNSW_MAX_M: usize = (u16::MAX as usize) / 2;
const HNSW_IDENTITY_MAGIC: &[u8; 4] = b"HNSI";
const HNSW_IDENTITY_VERSION: u32 = 1;
const HNSW_IDENTITY_HEADER_LEN: usize = 49;

// ─────────────────────────────────────────────────────────────
// Distance metric
// ─────────────────────────────────────────────────────────────

/// Distance metric for HNSW index construction and search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HnswDistanceMetric {
    /// Euclidean (L2) distance — default, works well for general embeddings
    L2 = 0,
    /// Cosine distance (1 - cosine_similarity) — for normalized embeddings
    Cosine = 1,
    /// Negative inner product — maximizes dot product similarity
    InnerProduct = 2,
}

impl HnswDistanceMetric {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::L2),
            1 => Some(Self::Cosine),
            2 => Some(Self::InnerProduct),
            _ => None,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "l2" | "euclidean" => Some(Self::L2),
            "cosine" => Some(Self::Cosine),
            "ip" | "inner_product" | "dot" => Some(Self::InnerProduct),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Heap helpers for HNSW search
// ─────────────────────────────────────────────────────────────

/// Max-heap entry (farthest first — for pruning the result set)
struct MaxEntry {
    distance: f32,
    node: u32,
}

impl PartialEq for MaxEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for MaxEntry {}
impl PartialOrd for MaxEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MaxEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.total_cmp(&other.distance)
    }
}

/// Min-heap entry (closest first — for candidate exploration)
struct MinEntry {
    distance: f32,
    node: u32,
}

impl PartialEq for MinEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for MinEntry {}
impl PartialOrd for MinEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MinEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed for min-heap behavior in BinaryHeap (which is max-heap)
        other.distance.total_cmp(&self.distance)
    }
}

// ─────────────────────────────────────────────────────────────
// HNSW Graph
// ─────────────────────────────────────────────────────────────

struct HnswNode {
    /// Neighbors at each layer: neighbors[layer] = Vec<(node_id, distance_to_this_node)>
    /// Storing distances avoids recomputation during neighbor pruning (90% of build time).
    neighbors: Vec<Vec<(u32, f32)>>,
}

/// Reusable scratch buffers for search_layer to avoid per-call allocations.
/// Uses a bitset for visited tracking — 1 bit per node (32x smaller than u32 generation
/// array) keeps the working set in L1 cache for dramatically fewer cache misses.
struct SearchScratch {
    /// Bitset for visited nodes: bit N = node N has been visited this call
    visited: Vec<u64>,
    /// Reusable min-heap for candidate exploration
    candidates: BinaryHeap<MinEntry>,
    /// Reusable max-heap for result pruning
    result: BinaryHeap<MaxEntry>,
}

impl SearchScratch {
    fn new() -> Self {
        Self {
            visited: Vec::new(),
            candidates: BinaryHeap::new(),
            result: BinaryHeap::new(),
        }
    }

    /// Prepare for a new search_layer call. Grows bitset if needed, clears visited bits.
    #[inline]
    fn reset(&mut self, node_count: usize) {
        let words = (node_count + 63) >> 6;
        if self.visited.len() < words {
            self.visited.resize(words, 0);
        } else {
            // Clear only the portion we need — memset is fast for L1-sized data
            // (12.5KB for 100K nodes, ~200ns to clear)
            self.visited[..words].fill(0);
        }
        self.candidates.clear();
        self.result.clear();
    }
}

/// Reusable scratch buffers for query-time search_layer calls.
/// Uses bitset for visited tracking (1 bit/node) — 32x smaller than FxHashSet,
/// keeps working set in L1 cache for dramatically fewer cache misses.
/// Thread-local storage avoids per-call heap allocation after the first invocation.
struct QueryScratch {
    /// Bitset for visited nodes: bit N = node N has been visited this call
    visited: Vec<u64>,
    candidates: BinaryHeap<MinEntry>,
    result: BinaryHeap<MaxEntry>,
}

impl QueryScratch {
    fn new() -> Self {
        Self {
            visited: Vec::new(),
            candidates: BinaryHeap::new(),
            result: BinaryHeap::new(),
        }
    }

    #[inline]
    fn reset(&mut self, node_count: usize) {
        let words = (node_count + 63) >> 6;
        if self.visited.len() < words {
            self.visited.resize(words, 0);
        } else {
            self.visited[..words].fill(0);
        }
        self.candidates.clear();
        self.result.clear();
    }
}

thread_local! {
    static QUERY_SCRATCH: std::cell::RefCell<QueryScratch> =
        std::cell::RefCell::new(QueryScratch::new());
}

// Bitset-based scratch for parallel build (zero-overhead visited tracking).
// Uses Vec<u64> bitset (1 bit/node) — 32x smaller than generation array, fits L1 cache.
#[cfg(feature = "parallel")]
thread_local! {
    static BUILD_SCRATCH: std::cell::RefCell<SearchScratch> =
        std::cell::RefCell::new(SearchScratch::new());
}

struct HnswInner {
    /// Graph nodes
    nodes: Vec<HnswNode>,
    /// Entry point node (highest layer)
    entry_point: Option<u32>,
    /// Current maximum layer
    max_layer: usize,
    /// Vector data: contiguous packed LE f32 bytes
    /// Node i's vector is at [i * dims_bytes .. (i+1) * dims_bytes]
    vectors: Vec<u8>,
    /// Dimension in bytes (dims * 4)
    dims_bytes: usize,
    /// Mapping: node_index → row_id
    node_to_row_id: Vec<i64>,
    /// Mapping: row_id → node_index
    row_id_to_node: ahash::AHashMap<i64, u32>,
    /// Distance metric used for graph construction
    metric: HnswDistanceMetric,
    /// Reusable scratch buffers for build-time search_layer calls
    scratch: SearchScratch,
    /// Deleted tombstone bitset — separated from HnswNode to avoid loading the 24-byte
    /// node struct just to check a bool in the hot search loop. 1 bit per node, ~1.6KB
    /// for 100K nodes, fits L1 cache. Avoids ~100ns cache misses per neighbor check.
    deleted_bits: Vec<u64>,
    /// O(1) exact-duplicate lookup for UNIQUE enforcement.
    /// Maps ahash(raw_vec_bytes) → list of live row_ids with that hash.
    /// `None` when UNIQUE is not enabled; built via `build_unique_map()`.
    unique_map: Option<ahash::AHashMap<u64, Vec<i64>>>,
    /// Stable topology seed derived from the index identity. Levels are a pure
    /// function of this seed and row ID, so rebuild/replay does not depend on
    /// process-global randomness or insertion scheduling.
    build_seed: u64,
}

impl HnswInner {
    #[cfg(test)]
    fn new(dims: usize, metric: HnswDistanceMetric) -> Self {
        Self::with_build_seed(dims, metric, 0x7261_6469_7864_6231)
    }

    fn with_build_seed(dims: usize, metric: HnswDistanceMetric, build_seed: u64) -> Self {
        Self {
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            vectors: Vec::new(),
            dims_bytes: dims
                .checked_mul(std::mem::size_of::<f32>())
                .expect("validated HNSW dimensions"),
            node_to_row_id: Vec::new(),
            row_id_to_node: ahash::AHashMap::new(),
            metric,
            scratch: SearchScratch::new(),
            deleted_bits: Vec::new(),
            unique_map: None,
            build_seed,
        }
    }

    /// Hash raw vector bytes for the unique_map.
    #[inline]
    fn hash_vec_bytes(bytes: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        bytes.hash(&mut hasher);
        hasher.finish()
    }

    /// Build the unique_map from all live (non-deleted) vectors.
    fn build_unique_map(&mut self) {
        let mut map: ahash::AHashMap<u64, Vec<i64>> =
            ahash::AHashMap::with_capacity(self.nodes.len());
        for (node_idx, &row_id) in self.node_to_row_id.iter().enumerate() {
            if self.is_deleted(node_idx as u32) {
                continue;
            }
            let offset = node_idx * self.dims_bytes;
            if let Some(vec_bytes) = self.vectors.get(offset..offset + self.dims_bytes) {
                let hash = Self::hash_vec_bytes(vec_bytes);
                map.entry(hash).or_default().push(row_id);
            }
        }
        self.unique_map = Some(map);
    }

    /// Add a node to the unique_map (if it exists).
    #[inline]
    fn unique_map_insert(&mut self, node_id: u32) {
        if let Some(ref mut map) = self.unique_map {
            let row_id = self.node_to_row_id[node_id as usize];
            let offset = node_id as usize * self.dims_bytes;
            let hash = Self::hash_vec_bytes(&self.vectors[offset..offset + self.dims_bytes]);
            map.entry(hash).or_default().push(row_id);
        }
    }

    /// Remove a node from the unique_map (if it exists).
    #[inline]
    fn unique_map_remove(&mut self, node_id: u32) {
        if let Some(ref mut map) = self.unique_map {
            let row_id = self.node_to_row_id[node_id as usize];
            let offset = node_id as usize * self.dims_bytes;
            let hash = Self::hash_vec_bytes(&self.vectors[offset..offset + self.dims_bytes]);
            if let Some(row_ids) = map.get_mut(&hash) {
                row_ids.retain(|&r| r != row_id);
                if row_ids.is_empty() {
                    map.remove(&hash);
                }
            }
        }
    }

    /// Check if a node is deleted (bitset lookup — L1 cache friendly)
    #[inline(always)]
    fn is_deleted(&self, node: u32) -> bool {
        let idx = node as usize;
        // SAFETY: node < nodes.len(), and deleted_bits is sized to cover all nodes via push_node_alive.
        unsafe { (*self.deleted_bits.get_unchecked(idx >> 6) >> (idx & 63)) & 1 != 0 }
    }

    /// Mark a node as deleted in the bitset
    #[inline(always)]
    fn set_deleted(&mut self, node: u32) {
        let idx = node as usize;
        // SAFETY: node < nodes.len(), and deleted_bits is sized to cover all nodes via push_node_alive.
        unsafe {
            *self.deleted_bits.get_unchecked_mut(idx >> 6) |= 1u64 << (idx & 63);
        }
    }

    /// Clear the deleted flag for a node (used on reinsert)
    #[inline(always)]
    fn clear_deleted(&mut self, node: u32) {
        let idx = node as usize;
        // SAFETY: node < nodes.len(), and deleted_bits is sized to cover all nodes via push_node_alive.
        unsafe {
            *self.deleted_bits.get_unchecked_mut(idx >> 6) &= !(1u64 << (idx & 63));
        }
    }

    /// Extend deleted bitset to accommodate a new node (always undeleted initially)
    #[inline]
    fn push_node_alive(&mut self) {
        let node_count = self.nodes.len(); // already pushed to nodes Vec
        let words_needed = (node_count + 63) >> 6;
        if self.deleted_bits.len() < words_needed {
            self.deleted_bits.resize(words_needed, 0);
        }
        // The new bit is already 0 (alive) from the resize or from previous state
    }

    /// Get the vector bytes for a node
    #[inline]
    fn vector_bytes(&self, node: u32) -> &[u8] {
        let start = node as usize * self.dims_bytes;
        &self.vectors[start..start + self.dims_bytes]
    }

    /// Get the vector as an f32 slice for a node (zero-copy reinterpret)
    #[inline]
    fn vector_f32(&self, node: u32) -> &[f32] {
        as_f32_slice(self.vector_bytes(node))
    }

    /// Compute distance between a node's vector and a query f32 slice
    #[inline]
    fn distance_f32(&self, node: u32, query: &[f32]) -> f32 {
        let v = self.vector_f32(node);
        match self.metric {
            HnswDistanceMetric::L2 => l2_distance_sq_f32(v, query),
            HnswDistanceMetric::Cosine => cosine_distance_f32(v, query),
            HnswDistanceMetric::InnerProduct => ip_distance_f32(v, query),
        }
    }

    /// Search a single layer for ef nearest neighbors (read-only, uses thread-local scratch).
    /// Uses bitset visited tracking (1 bit/node) and prefetch hints for cache-friendly traversal.
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[u32],
        ef: usize,
        layer: usize,
    ) -> Vec<MaxEntry> {
        QUERY_SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            let node_count = self.nodes.len();
            scratch.reset(node_count);

            let visited_ptr = scratch.visited.as_mut_ptr();
            let deleted_ptr = self.deleted_bits.as_ptr();
            let vectors_ptr = self.vectors.as_ptr();
            let dims_bytes = self.dims_bytes;
            let dims = dims_bytes / 4;
            let metric = self.metric;

            for &ep in entry_points {
                if (ep as usize) >= node_count {
                    continue;
                }
                let n_idx = ep as usize;
                // SAFETY: visited is sized to node_count bits; n_idx < node_count (checked above).
                unsafe {
                    *visited_ptr.add(n_idx >> 6) |= 1u64 << (n_idx & 63);
                }
                // SAFETY: n_idx < node_count, so n_idx * dims_bytes is within the vectors buffer.
                let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
                // SAFETY: vectors buffer stores dims f32 values per node; pointer is aligned (system allocator).
                let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
                let d = match metric {
                    HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                    HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                    HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
                };
                scratch.candidates.push(MinEntry {
                    distance: d,
                    node: ep,
                });
                scratch.result.push(MaxEntry {
                    distance: d,
                    node: ep,
                });
            }

            let mut farthest_dist = scratch.result.peek().map_or(f32::MAX, |e| e.distance);
            let mut result_len = scratch.result.len();

            let nodes_ptr = self.nodes.as_ptr();
            let nodes_len = self.nodes.len();

            while let Some(MinEntry {
                distance: c_dist,
                node: c_id,
            }) = scratch.candidates.pop()
            {
                if c_dist > farthest_dist && result_len >= ef {
                    break;
                }

                // Prefetch the NEXT candidate's node struct while we process this one
                if let Some(next) = scratch.candidates.peek() {
                    let next_id = next.node as usize;
                    if next_id < nodes_len {
                        // SAFETY: next_id < nodes_len, so the pointer is within the nodes slice.
                        let next_node_ptr = unsafe { nodes_ptr.add(next_id) } as *const u8;
                        prefetch_read(next_node_ptr);
                    }
                }

                // SAFETY: c_id was inserted from a valid node index; all node indices < nodes_len.
                let node = unsafe { &*nodes_ptr.add(c_id as usize) };
                if layer < node.neighbors.len() {
                    let neighbors = &node.neighbors[layer];
                    let nlen = neighbors.len();
                    let nptr = neighbors.as_ptr();

                    let mut ni = 0usize;
                    while ni < nlen {
                        // SAFETY: ni < nlen, so the pointer is within the neighbors slice.
                        let (neighbor, _) = unsafe { *nptr.add(ni) };
                        ni += 1;

                        let n_idx = neighbor as usize;

                        // Prefetch the NEXT neighbor's visited bit
                        if ni < nlen {
                            // SAFETY: ni < nlen, pointer within neighbors slice.
                            let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                            let next_v_word = next_nb >> 6;
                            // SAFETY: next_v_word < visited word count since neighbors are valid node indices.
                            prefetch_read(unsafe { visited_ptr.add(next_v_word) } as *const u8);
                        }

                        let v_word_idx = n_idx >> 6;
                        let v_bit_mask = 1u64 << (n_idx & 63);

                        // SAFETY: n_idx is a valid node index < node_count; visited is sized to node_count bits.
                        let v_word_ptr = unsafe { visited_ptr.add(v_word_idx) };
                        // SAFETY: v_word_ptr is valid (derived from visited_ptr with in-bounds offset above).
                        let v_word = unsafe { *v_word_ptr };
                        if (v_word & v_bit_mask) != 0 {
                            continue;
                        }
                        // SAFETY: v_word_ptr points to a valid visited word (same as the read above).
                        unsafe {
                            *v_word_ptr = v_word | v_bit_mask;
                        }

                        // SAFETY: v_word_idx < deleted_bits word count; deleted_bits covers all node indices.
                        let is_deleted =
                            unsafe { (*deleted_ptr.add(v_word_idx)) & v_bit_mask != 0 };
                        if is_deleted {
                            continue;
                        }

                        // Prefetch the NEXT neighbor's vector data
                        if ni < nlen {
                            // SAFETY: ni < nlen, pointer within neighbors slice.
                            let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                            // SAFETY: next_nb is a valid node index; next_nb * dims_bytes is within vectors.
                            let vec_addr = unsafe { vectors_ptr.add(next_nb * dims_bytes) };
                            prefetch_read(vec_addr);
                        }

                        // SAFETY: n_idx < node_count; n_idx * dims_bytes is within the vectors buffer.
                        let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
                        // SAFETY: vectors stores dims f32 values per node; pointer is aligned (system allocator).
                        let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
                        let d = match metric {
                            HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                            HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                            HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
                        };

                        if d < farthest_dist || result_len < ef {
                            scratch.candidates.push(MinEntry {
                                distance: d,
                                node: neighbor,
                            });
                            scratch.result.push(MaxEntry {
                                distance: d,
                                node: neighbor,
                            });
                            result_len += 1;
                            if result_len > ef {
                                scratch.result.pop();
                                result_len -= 1;
                                farthest_dist =
                                    scratch.result.peek().map_or(f32::MAX, |e| e.distance);
                            } else {
                                farthest_dist = farthest_dist.max(d);
                            }
                        }
                    }
                }
            }

            scratch.result.drain().collect()
        })
    }

    /// Select the best M neighbors using the HNSW diversity heuristic (Algorithm 4).
    ///
    /// Instead of simply picking the M closest candidates, this prefers neighbors
    /// that are diverse — a candidate is kept only if it is closer to the query
    /// than to any already-selected neighbor. This produces a more navigable graph
    /// with better recall, especially in clustered data.
    ///
    /// After the diversity pass, remaining slots are filled with the closest
    /// unused candidates to ensure we always return up to M neighbors.
    fn select_neighbors(&self, mut candidates: Vec<MaxEntry>, m: usize) -> Vec<(u32, f32)> {
        if candidates.is_empty() || m == 0 {
            return Vec::new();
        }

        // Sort in-place (no allocation — candidates is owned)
        candidates.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));

        let mut selected: Vec<(u32, f32)> = Vec::with_capacity(m);
        let mut pruned: Vec<(u32, f32)> = Vec::with_capacity(candidates.len());

        // Phase 1: Diversity-aware selection (Algorithm 4)
        // Keep a candidate if it is closer to the query than to any already-selected neighbor
        let metric = self.metric;
        for entry in &candidates {
            if selected.len() >= m {
                break;
            }
            let dist_to_query = entry.distance;
            // Hoist entry vector lookup outside inner loop (invariant across selected neighbors)
            let entry_vec = self.vector_f32(entry.node);
            let mut is_diverse = true;
            for &(sel_node, _) in &selected {
                let sel_vec = self.vector_f32(sel_node);
                let dist_to_selected = match metric {
                    HnswDistanceMetric::L2 => l2_distance_sq_f32(entry_vec, sel_vec),
                    HnswDistanceMetric::Cosine => cosine_distance_f32(entry_vec, sel_vec),
                    HnswDistanceMetric::InnerProduct => ip_distance_f32(entry_vec, sel_vec),
                };
                if dist_to_selected < dist_to_query {
                    is_diverse = false;
                    break;
                }
            }
            if is_diverse {
                selected.push((entry.node, entry.distance));
            } else {
                pruned.push((entry.node, entry.distance));
            }
        }

        // Phase 2: Fill remaining slots from pruned candidates (keepPrunedConnections)
        for entry in pruned {
            if selected.len() >= m {
                break;
            }
            selected.push(entry);
        }

        selected
    }

    /// Search a single layer using reusable scratch buffers (for build-time use with &mut self).
    /// Same algorithm as search_layer but avoids per-call allocations.
    fn search_layer_mut(
        &mut self,
        query: &[f32],
        entry_points: &[u32],
        ef: usize,
        layer: usize,
    ) -> Vec<MaxEntry> {
        let node_count = self.nodes.len();
        // Extract scratch to allow independent borrowing of self.nodes and scratch
        // (eliminates the neighbor-copy workaround for borrow-checker conflict)
        let mut scratch = std::mem::replace(&mut self.scratch, SearchScratch::new());
        scratch.reset(node_count);

        let visited_ptr = scratch.visited.as_mut_ptr();
        let deleted_ptr = self.deleted_bits.as_ptr();
        let vectors_ptr = self.vectors.as_ptr();
        let dims_bytes = self.dims_bytes;
        let dims = dims_bytes / 4;
        let metric = self.metric;

        for &ep in entry_points {
            if (ep as usize) >= node_count {
                continue;
            }
            let n_idx = ep as usize;
            // SAFETY: visited is sized to node_count bits; n_idx < node_count (checked above).
            unsafe {
                *visited_ptr.add(n_idx >> 6) |= 1u64 << (n_idx & 63);
            }
            // SAFETY: n_idx < node_count, so n_idx * dims_bytes is within the vectors buffer.
            let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
            // SAFETY: vectors buffer stores dims f32 values per node; pointer is aligned (system allocator).
            let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
            let d = match metric {
                HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
            };
            scratch.candidates.push(MinEntry {
                distance: d,
                node: ep,
            });
            scratch.result.push(MaxEntry {
                distance: d,
                node: ep,
            });
        }

        // Cache farthest distance and result count in locals to avoid
        // BinaryHeap::peek + Vec::len calls every iteration (saves ~5s from profiling)
        let mut farthest_dist = scratch.result.peek().map_or(f32::MAX, |e| e.distance);
        let mut result_len = scratch.result.len();

        let nodes_ptr = self.nodes.as_ptr();
        let nodes_len = self.nodes.len();

        while let Some(MinEntry {
            distance: c_dist,
            node: c_id,
        }) = scratch.candidates.pop()
        {
            if c_dist > farthest_dist && result_len >= ef {
                break;
            }

            // Prefetch the NEXT candidate's node struct while we process this one.
            // Hides the ~100ns L2 latency of the 2-pointer-chase: node -> neighbors Vec -> data.
            if let Some(next) = scratch.candidates.peek() {
                let next_id = next.node as usize;
                if next_id < nodes_len {
                    // SAFETY: next_id < nodes_len, so the pointer is within the nodes slice.
                    let next_node_ptr = unsafe { nodes_ptr.add(next_id) } as *const u8;
                    prefetch_read(next_node_ptr);
                }
            }

            // SAFETY: c_id was inserted from a valid node index; all node indices < nodes_len.
            let node = unsafe { &*nodes_ptr.add(c_id as usize) };
            if layer < node.neighbors.len() {
                let neighbors = &node.neighbors[layer];
                let nlen = neighbors.len();
                let nptr = neighbors.as_ptr();

                let mut ni = 0usize;
                while ni < nlen {
                    // SAFETY: ni < nlen, so the pointer is within the neighbors slice.
                    let (neighbor, _) = unsafe { *nptr.add(ni) };
                    ni += 1;

                    let n_idx = neighbor as usize;

                    // Prefetch the NEXT neighbor's visited bit to hide L2 latency
                    if ni < nlen {
                        // SAFETY: ni < nlen, pointer within neighbors slice.
                        let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                        let next_v_word = next_nb >> 6;
                        // SAFETY: next_v_word < visited word count since neighbors are valid node indices.
                        prefetch_read(unsafe { visited_ptr.add(next_v_word) } as *const u8);
                    }

                    let v_word_idx = n_idx >> 6;
                    let v_bit_mask = 1u64 << (n_idx & 63);

                    // SAFETY: n_idx is a valid node index < node_count; visited is sized to node_count bits.
                    let v_word_ptr = unsafe { visited_ptr.add(v_word_idx) };
                    // SAFETY: v_word_ptr is valid (derived from visited_ptr with in-bounds offset above).
                    let v_word = unsafe { *v_word_ptr };
                    if (v_word & v_bit_mask) != 0 {
                        continue;
                    }
                    // SAFETY: v_word_ptr points to a valid visited word (same as the read above).
                    unsafe {
                        *v_word_ptr = v_word | v_bit_mask;
                    }

                    // SAFETY: v_word_idx < deleted_bits word count; deleted_bits covers all node indices.
                    let is_deleted = unsafe { (*deleted_ptr.add(v_word_idx)) & v_bit_mask != 0 };
                    if is_deleted {
                        continue;
                    }

                    // Prefetch the NEXT neighbor's vector data while we compute
                    // the current distance -- hides ~100ns of L2/L3 cache miss latency
                    if ni < nlen {
                        // SAFETY: ni < nlen, pointer within neighbors slice.
                        let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                        // SAFETY: next_nb is a valid node index; next_nb * dims_bytes is within vectors.
                        let vec_addr = unsafe { vectors_ptr.add(next_nb * dims_bytes) };
                        prefetch_read(vec_addr);
                    }

                    // SAFETY: n_idx < node_count; n_idx * dims_bytes is within the vectors buffer.
                    let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
                    // SAFETY: vectors stores dims f32 values per node; pointer is aligned (system allocator).
                    let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
                    let d = match metric {
                        HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                        HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                        HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
                    };

                    if d < farthest_dist || result_len < ef {
                        scratch.candidates.push(MinEntry {
                            distance: d,
                            node: neighbor,
                        });
                        scratch.result.push(MaxEntry {
                            distance: d,
                            node: neighbor,
                        });
                        result_len += 1;
                        if result_len > ef {
                            scratch.result.pop();
                            result_len -= 1;
                            // Update cached farthest after eviction
                            farthest_dist = scratch.result.peek().map_or(f32::MAX, |e| e.distance);
                        } else {
                            // Keep farthest in sync while filling up to ef; otherwise break
                            // condition can become too aggressive and terminate exploration early.
                            farthest_dist = farthest_dist.max(d);
                        }
                    }
                }
            }
        }

        let result = scratch.result.drain().collect();
        self.scratch = scratch;
        result
    }

    /// Greedy descent: walk from entry point to the closest node at the given layer.
    /// Used for upper-layer descent (ef=1) — no heap, no visited set, no allocation.
    /// Equivalent to search_layer(query, &[ep], 1, layer) but ~10x faster:
    /// eliminates BinaryHeap push/pop, visited-set management, and Vec allocation.
    #[inline]
    fn greedy_closest(&self, query: &[f32], mut ep: u32, layer: usize) -> u32 {
        let mut ep_dist = self.distance_f32(ep, query);
        loop {
            let mut best_dist = ep_dist;
            let mut best_node = ep;
            let node = &self.nodes[ep as usize];
            if layer < node.neighbors.len() {
                for &(neighbor, _) in &node.neighbors[layer] {
                    if self.is_deleted(neighbor) {
                        continue;
                    }
                    let d = self.distance_f32(neighbor, query);
                    if d < best_dist {
                        best_dist = d;
                        best_node = neighbor;
                    }
                }
            }
            if best_node == ep {
                return ep;
            }
            ep = best_node;
            ep_dist = best_dist;
        }
    }

    /// Fast reverse-edge maintenance: dedupe + farthest replacement.
    /// O(M), optimized for bulk batch builds.
    fn update_connection_fast(
        &mut self,
        target: u32,
        new_nb: u32,
        new_dist: f32,
        layer: usize,
        max_conn: usize,
    ) {
        if max_conn == 0 || target == new_nb {
            return;
        }

        let neighbors = &self.nodes[target as usize].neighbors[layer];

        // Duplicate edge: keep the tighter distance.
        if let Some(existing_idx) = neighbors.iter().position(|&(nid, _)| nid == new_nb) {
            if new_dist < neighbors[existing_idx].1 {
                self.nodes[target as usize].neighbors[layer][existing_idx].1 = new_dist;
            }
            return;
        }

        let cur_len = neighbors.len();

        // Room available — just push.
        if cur_len < max_conn {
            self.nodes[target as usize].neighbors[layer].push((new_nb, new_dist));
            return;
        }

        let mut farthest_idx = 0;
        let mut farthest_dist = f32::MIN;
        for (i, &(_, d)) in neighbors.iter().enumerate() {
            if d > farthest_dist {
                farthest_dist = d;
                farthest_idx = i;
            }
        }
        if new_dist < farthest_dist {
            self.nodes[target as usize].neighbors[layer][farthest_idx] = (new_nb, new_dist);
        }
    }

    /// Add a reverse connection from `target` to `new_nb`.
    ///
    /// `prefer_diversity` enables a higher-quality but slower overflow policy on layer 0.
    /// Used for single-row inserts to keep navigability strong without penalizing
    /// high-throughput batch build paths.
    fn update_connection(
        &mut self,
        target: u32,
        new_nb: u32,
        new_dist: f32,
        layer: usize,
        max_conn: usize,
        prefer_diversity: bool,
    ) {
        if !prefer_diversity {
            self.update_connection_fast(target, new_nb, new_dist, layer, max_conn);
            return;
        }

        // Upper layers are sparse and inexpensive to maintain with fast replacement.
        if layer > 0 {
            self.update_connection_fast(target, new_nb, new_dist, layer, max_conn);
            return;
        }

        // Past this size, full diversity overflow pruning is too expensive in practice.
        const DIVERSITY_OVERFLOW_MAX_NODES: usize = 200_000;
        if self.nodes.len() > DIVERSITY_OVERFLOW_MAX_NODES {
            self.update_connection_fast(target, new_nb, new_dist, layer, max_conn);
            return;
        }

        if max_conn == 0 || target == new_nb {
            return;
        }
        let neighbors = &self.nodes[target as usize].neighbors[layer];

        if let Some(existing_idx) = neighbors.iter().position(|&(nid, _)| nid == new_nb) {
            if new_dist < neighbors[existing_idx].1 {
                self.nodes[target as usize].neighbors[layer][existing_idx].1 = new_dist;
            }
            return;
        }

        let cur_len = neighbors.len();
        if cur_len < max_conn {
            self.nodes[target as usize].neighbors[layer].push((new_nb, new_dist));
            return;
        }

        // Cheap guard: small improvements do not justify full diversity re-pruning.
        let mut farthest_idx = 0;
        let mut farthest_dist = f32::MIN;
        for (i, &(_, d)) in neighbors.iter().enumerate() {
            if d > farthest_dist {
                farthest_dist = d;
                farthest_idx = i;
            }
        }
        if new_dist >= farthest_dist {
            return;
        }
        if new_dist > farthest_dist * 0.90 {
            self.nodes[target as usize].neighbors[layer][farthest_idx] = (new_nb, new_dist);
            return;
        }

        // Layer 0 is critical for recall. On overflow, run diversity pruning
        // over existing + new candidate (hnswlib-style behavior).
        let mut candidates: Vec<MaxEntry> = Vec::with_capacity(cur_len + 1);
        for &(nid, dist) in neighbors {
            candidates.push(MaxEntry {
                distance: dist,
                node: nid,
            });
        }
        candidates.push(MaxEntry {
            distance: new_dist,
            node: new_nb,
        });

        let pruned = self.select_neighbors(candidates, max_conn);
        self.nodes[target as usize].neighbors[layer] = pruned;
    }

    /// Insert a vector into the graph
    fn insert(
        &mut self,
        vector_bytes: &[u8],
        row_id: i64,
        m: usize,
        m0: usize,
        ef_construction: usize,
        ml: f64,
    ) {
        // Check for existing mapping (duplicate or tombstoned reinsert)
        if let Some(&existing_node) = self.row_id_to_node.get(&row_id) {
            if self.is_deleted(existing_node) {
                // Reinsert: update vector data in place and clear tombstone
                let offset = existing_node as usize * self.dims_bytes;
                self.vectors[offset..offset + self.dims_bytes].copy_from_slice(vector_bytes);
                self.clear_deleted(existing_node);
                self.unique_map_insert(existing_node);
                // Reconnect into graph using the same pattern as fresh insert
                let query_vec = bytes_to_f32_vec(vector_bytes);
                let query = &query_vec;
                let level = self.nodes[existing_node as usize].neighbors.len() - 1;
                if let Some(ep) = self.entry_point {
                    let mut cur_ep = ep;
                    if level < self.max_layer {
                        for l in ((level + 1)..=self.max_layer).rev() {
                            cur_ep = self.greedy_closest(query, cur_ep, l);
                        }
                    }
                    let build_ef = self.effective_ef_construction(ef_construction, m);
                    let mut entry_points: Vec<u32> = vec![cur_ep];
                    let start_layer = level.min(self.max_layer);
                    for l in (0..=start_layer).rev() {
                        let candidates = self.search_layer_mut(query, &entry_points, build_ef, l);
                        let max_conn = if l == 0 { m0 } else { m };
                        let neighbors = self.select_neighbors(candidates, m);
                        for &(neighbor, dist) in &neighbors {
                            if l >= self.nodes[neighbor as usize].neighbors.len() {
                                continue;
                            }
                            self.update_connection(
                                neighbor,
                                existing_node,
                                dist,
                                l,
                                max_conn,
                                true,
                            );
                        }
                        entry_points.clear();
                        entry_points.extend(neighbors.iter().map(|&(n, _)| n));
                        self.nodes[existing_node as usize].neighbors[l] = neighbors;
                    }
                }
                return;
            }
            // Not deleted — true duplicate (snapshot + WAL replay), skip
            return;
        }
        let node_id = self.nodes.len() as u32;
        let level = deterministic_level(self.build_seed, row_id, ml);

        // Store vector data
        self.vectors.extend_from_slice(vector_bytes);

        // Create node
        self.nodes.push(HnswNode {
            neighbors: vec![Vec::new(); level + 1],
        });
        self.push_node_alive();

        // Store mappings
        self.node_to_row_id.push(row_id);
        self.row_id_to_node.insert(row_id, node_id);
        self.unique_map_insert(node_id);

        // First node — just set as entry point
        if node_id == 0 {
            self.entry_point = Some(0);
            self.max_layer = level;
            return;
        }

        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return,
        };

        // Convert potentially-unaligned query bytes to aligned f32 (done once per insert)
        let query_vec = bytes_to_f32_vec(vector_bytes);
        let query = &query_vec;

        // Greedy descent from top layer to node's level + 1 (fast-path, no heap/allocation)
        let mut cur_ep = ep;
        if level < self.max_layer {
            for l in ((level + 1)..=self.max_layer).rev() {
                cur_ep = self.greedy_closest(query, cur_ep, l);
            }
        }

        // Entry points for layer insertion (accumulates selected neighbors per layer)
        let mut entry_points: Vec<u32> = Vec::with_capacity(m.max(1));
        entry_points.push(cur_ep);
        let build_ef = self.effective_ef_construction(ef_construction, m);

        // Insert at each layer from min(level, max_layer) down to 0
        let start_layer = level.min(self.max_layer);
        for l in (0..=start_layer).rev() {
            let candidates = self.search_layer_mut(query, &entry_points, build_ef, l);
            let max_conn = if l == 0 { m0 } else { m };

            // hnswlib style: new node gets M diverse neighbors (not M0).
            // Existing nodes can accumulate up to M0 via reverse connections.
            let neighbors = self.select_neighbors(candidates, m);

            // Connect neighbors back to new node (bidirectional, same distance)
            for &(neighbor, dist) in &neighbors {
                if l >= self.nodes[neighbor as usize].neighbors.len() {
                    continue;
                }
                self.update_connection(neighbor, node_id, dist, l, max_conn, true);
            }

            // Update entry points for next layer, then store neighbors
            entry_points.clear();
            entry_points.extend(neighbors.iter().map(|&(n, _)| n));
            self.nodes[node_id as usize].neighbors[l] = neighbors;
        }

        // Update entry point if new node has higher level
        if level > self.max_layer {
            self.max_layer = level;
            self.entry_point = Some(node_id);
        }
    }

    /// Search for k nearest neighbors
    fn search(&self, query_bytes: &[u8], k: usize, ef_search: usize) -> Vec<(i64, f64)> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return Vec::new(),
        };

        // Convert potentially-unaligned query bytes to aligned f32 (done once per search)
        let query_vec = bytes_to_f32_vec(query_bytes);
        let query = &query_vec;
        let dynamic_ef_floor = if self.nodes.len() >= 1_000_000 {
            768
        } else if self.nodes.len() >= 500_000 {
            640
        } else if self.nodes.len() >= 100_000 {
            512
        } else {
            0
        };
        let ef = ef_search.max(k).max(dynamic_ef_floor);

        // Greedy descent from top layer to layer 1 (fast-path, no heap/allocation)
        let mut cur_ep = ep;
        for l in (1..=self.max_layer).rev() {
            cur_ep = self.greedy_closest(query, cur_ep, l);
        }

        // Search layer 0 with full ef
        let mut results = self.search_layer(query, std::slice::from_ref(&cur_ep), ef, 0);

        // HNSW distances guide navigation only. Recompute every returned
        // candidate with the public scalar f64 contract, then rerank that
        // candidate set before publication.
        results.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));

        let metric = self.metric;
        let mut exact_results: Vec<(i64, f64)> = results
            .into_iter()
            .filter(|e| !self.is_deleted(e.node))
            .map(|e| {
                let row_id = self.node_to_row_id[e.node as usize];
                let offset = e.node as usize * self.dims_bytes;
                let vector = &self.vectors[offset..offset + self.dims_bytes];
                let final_dist = match metric {
                    HnswDistanceMetric::L2 => {
                        radixdb_core::vector::l2_distance_bytes(vector, query_bytes)
                    }
                    HnswDistanceMetric::Cosine => {
                        radixdb_core::vector::cosine_distance_bytes(vector, query_bytes)
                    }
                    HnswDistanceMetric::InnerProduct => {
                        radixdb_core::vector::ip_distance_bytes(vector, query_bytes)
                    }
                };
                (row_id, final_dist.unwrap_or(f64::INFINITY))
            })
            .collect();
        exact_results.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        exact_results.truncate(k);
        exact_results
    }

    #[inline]
    fn effective_ef_construction(&self, requested: usize, m: usize) -> usize {
        let n = self.nodes.len();
        let floor = if n >= 1_000_000 {
            m.saturating_mul(10)
        } else if n >= 300_000 {
            m.saturating_mul(8)
        } else if n >= 100_000 {
            m.saturating_mul(6)
        } else {
            requested
        };
        requested.max(floor).min(requested.saturating_mul(2))
    }

    #[cfg(feature = "parallel")]
    #[inline]
    fn parallel_seed_count(total: usize) -> usize {
        // For million-scale builds, sqrt(N) alone under-seeds the graph.
        // Use max(sqrt(N), 0.25% of N), clamped to avoid over-seeding.
        let sqrt_seed = (total as f64).sqrt() as usize;
        let frac_seed = total / 400;
        sqrt_seed.max(frac_seed).clamp(256, 4_000).min(total)
    }

    #[cfg(feature = "parallel")]
    #[inline]
    fn parallel_batch_size(node_count: usize) -> usize {
        if node_count < 100_000 {
            512
        } else if node_count < 400_000 {
            1024
        } else {
            1536
        }
    }

    /// Parallel batch insert: seeds graph sequentially, then inserts remaining nodes
    /// using batched parallel search + sequential connect.
    #[cfg(feature = "parallel")]
    fn insert_batch_parallel(
        &mut self,
        entries: &[(&[u8], i64)],
        m: usize,
        m0: usize,
        ef_construction: usize,
        ml: f64,
    ) {
        const PARALLEL_THRESHOLD: usize = 5000;

        if entries.len() < PARALLEL_THRESHOLD {
            for &(vec_bytes, row_id) in entries {
                self.insert(vec_bytes, row_id, m, m0, ef_construction, ml);
            }
            return;
        }

        // Phase 1: Stronger seed improves navigability and recall at 1M+ scale.
        let seed_count = Self::parallel_seed_count(entries.len());
        for &(vec_bytes, row_id) in &entries[..seed_count] {
            self.insert(vec_bytes, row_id, m, m0, ef_construction, ml);
        }

        // Phase 2: Adaptive batches: smaller early for quality, larger later for throughput.
        let mut offset = seed_count;
        while offset < entries.len() {
            let batch_size = Self::parallel_batch_size(self.nodes.len());
            let end = (offset + batch_size).min(entries.len());
            self.insert_batch_inner(&entries[offset..end], m, m0, ef_construction, ml);
            offset = end;
        }
    }

    /// Inner batch insert: register nodes, handle upper layers sequentially,
    /// search layer 0 in parallel, connect sequentially.
    #[cfg(feature = "parallel")]
    fn insert_batch_inner(
        &mut self,
        batch: &[(&[u8], i64)],
        m: usize,
        m0: usize,
        ef_construction: usize,
        ml: f64,
    ) {
        use rayon::prelude::*;

        // Step 1: Register all nodes in batch (sequential)
        // (node_id, level) — vectors are already stored in self.vectors, accessed via vector_f32()
        let mut batch_nodes: Vec<(u32, usize)> = Vec::with_capacity(batch.len());

        for &(vec_bytes, row_id) in batch {
            if let Some(&existing_node) = self.row_id_to_node.get(&row_id) {
                if self.is_deleted(existing_node) {
                    // Reinsert: update vector data in place and clear tombstone
                    let offset = existing_node as usize * self.dims_bytes;
                    self.vectors[offset..offset + self.dims_bytes].copy_from_slice(vec_bytes);
                    self.clear_deleted(existing_node);
                    self.unique_map_insert(existing_node);
                    let level = self.nodes[existing_node as usize].neighbors.len() - 1;
                    if self.entry_point.is_some() {
                        batch_nodes.push((existing_node, level));
                    }
                }
                // Not deleted — true duplicate, skip
                continue;
            }
            let node_id = self.nodes.len() as u32;
            let level = deterministic_level(self.build_seed, row_id, ml);

            self.vectors.extend_from_slice(vec_bytes);
            self.nodes.push(HnswNode {
                neighbors: vec![Vec::new(); level + 1],
            });
            self.push_node_alive();
            self.node_to_row_id.push(row_id);
            self.row_id_to_node.insert(row_id, node_id);
            self.unique_map_insert(node_id);

            if self.entry_point.is_none() {
                self.entry_point = Some(node_id);
                self.max_layer = level;
                continue;
            }

            batch_nodes.push((node_id, level));
        }

        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return,
        };
        let fast_bulk_mode = self.nodes.len() >= 300_000;

        // Reusable query buffer — avoids allocating Vec<f32> per node in Steps 2 & 3
        let dims = self.dims_bytes / 4;
        let mut query_buf: Vec<f32> = Vec::with_capacity(dims);
        let build_ef = self.effective_ef_construction(ef_construction, m);
        let build_ef = if self.nodes.len() >= 100_000 {
            build_ef.min(128)
        } else {
            build_ef
        };

        // Step 2: Compute entry points for ALL nodes on the CLEAN graph
        // (before any batch upper-layer connections that would create dead-end routes)
        struct SearchTask {
            node_id: u32,
            level: usize,
            entry_point: u32,
        }

        let mut tasks: Vec<SearchTask> = Vec::with_capacity(batch_nodes.len());
        for &(node_id, level) in &batch_nodes {
            let entry_point = if fast_bulk_mode {
                ep
            } else {
                let mut entry_point = ep;
                // Greedy descent from top layer to layer 1 (fast-path, no heap/allocation)
                // Pass vector_f32 directly — both are &self borrows, no copy needed
                if self.max_layer > 0 {
                    let query = self.vector_f32(node_id);
                    for l in (1..=self.max_layer).rev() {
                        entry_point = self.greedy_closest(query, entry_point, l);
                    }
                }
                entry_point
            };

            tasks.push(SearchTask {
                node_id,
                level,
                entry_point,
            });
        }

        // Step 3: Handle upper-layer connections sequentially (rare nodes, <10%)
        if !fast_bulk_mode {
            for task in &tasks {
                if task.level == 0 {
                    continue;
                }
                let node_id = task.node_id;
                let level = task.level;
                query_buf.clear();
                query_buf.extend_from_slice(self.vector_f32(node_id));

                // Greedy descent from top to level+1 (fast-path, no heap/allocation)
                let mut cur_ep = ep;
                if level < self.max_layer {
                    for l in ((level + 1)..=self.max_layer).rev() {
                        cur_ep = self.greedy_closest(&query_buf, cur_ep, l);
                    }
                }

                let mut entry_points: Vec<u32> = Vec::with_capacity(m.max(1));
                entry_points.push(cur_ep);

                // Connect upper layers (level down to 1)
                let start_layer = level.min(self.max_layer);
                for l in (1..=start_layer).rev() {
                    let candidates = self.search_layer_mut(&query_buf, &entry_points, build_ef, l);
                    let max_conn = m;
                    let neighbors = self.select_neighbors(candidates, max_conn);

                    for &(neighbor, dist) in &neighbors {
                        if l >= self.nodes[neighbor as usize].neighbors.len() {
                            continue;
                        }
                        self.update_connection(neighbor, node_id, dist, l, max_conn, false);
                    }
                    entry_points.clear();
                    entry_points.extend(neighbors.iter().map(|&(n, _)| n));
                    self.nodes[node_id as usize].neighbors[l] = neighbors;
                }

                // Update entry point if this node has higher level
                if level > self.max_layer {
                    self.max_layer = level;
                    self.entry_point = Some(node_id);
                }
            }
        }

        // Step 4: Parallel layer 0 search (using clean entry points from Step 2)
        let batch_ef = build_ef;
        let results: Vec<(u32, Vec<(u32, f32)>)>;
        {
            let nodes = &self.nodes;
            let vectors = &self.vectors;
            let dims_bytes = self.dims_bytes;
            let metric = self.metric;
            let deleted_bits = &self.deleted_bits;

            results = tasks
                .par_iter()
                .map(|task| {
                    let start = task.node_id as usize * dims_bytes;
                    let query = as_f32_slice(&vectors[start..start + dims_bytes]);
                    let candidates = search_layer_shared(
                        nodes,
                        deleted_bits,
                        vectors,
                        dims_bytes,
                        metric,
                        query,
                        std::slice::from_ref(&task.entry_point),
                        batch_ef,
                        0,
                    );
                    // hnswlib style: new node gets M diverse neighbors (not M0)
                    let neighbors =
                        select_neighbors_shared(vectors, dims_bytes, metric, candidates, m);
                    (task.node_id, neighbors)
                })
                .collect();
        } // immutable borrows dropped here

        // Step 5: Sequential layer 0 connect.
        // For each node: first add reverse edges to its forward neighbors,
        // then set its own forward connections. This intentionally overwrites
        // any reverse edges other nodes added to this node — forward connections
        // (from diversity-pruned search) are higher quality than reverse edges.
        for (node_id, neighbors) in results {
            for &(nb, dist) in &neighbors {
                if self.nodes[nb as usize].neighbors.is_empty() {
                    continue;
                }
                self.update_connection(nb, node_id, dist, 0, m0, false);
            }
            self.nodes[node_id as usize].neighbors[0] = neighbors;
        }
    }

    /// Serialize the HNSW graph to bytes for snapshot persistence.
    ///
    /// Binary format:
    /// ```text
    /// [4B] magic "HNSW"
    /// [4B] version (2)
    /// [1B] metric
    /// [4B] dims_bytes
    /// [4B] node_count
    /// [4B] max_layer
    /// [4B] entry_point (u32::MAX if None)
    /// [node_count * dims_bytes] vectors (contiguous)
    /// [node_count * 8] row_ids (i64 LE)
    /// Per node:
    ///   [1B] deleted flag
    ///   [1B] num_layers
    ///   Per layer:
    ///     [2B] neighbor_count
    ///     [neighbor_count * 8B] neighbor node ID and distance pairs
    /// ```
    fn serialize_graph(&self) -> std::result::Result<Vec<u8>, String> {
        let node_count = self.nodes.len();
        let dims_width = u32::try_from(self.dims_bytes)
            .map_err(|_| "HNSW dimensions exceed persisted u32 width".to_string())?;
        let node_width = u32::try_from(node_count)
            .map_err(|_| "HNSW node count exceeds persisted u32 width".to_string())?;
        let max_layer_width = u32::try_from(self.max_layer)
            .map_err(|_| "HNSW max layer exceeds persisted u32 width".to_string())?;
        let expected_vector_bytes = node_count
            .checked_mul(self.dims_bytes)
            .ok_or_else(|| "HNSW vector byte count overflow".to_string())?;
        if self.vectors.len() != expected_vector_bytes || self.node_to_row_id.len() != node_count {
            return Err("HNSW graph authorities have inconsistent lengths".to_string());
        }
        let mut seen_row_ids = ahash::AHashSet::with_capacity(node_count);
        if self
            .node_to_row_id
            .iter()
            .any(|row_id| !seen_row_ids.insert(*row_id))
        {
            return Err("HNSW graph contains duplicate row IDs".to_string());
        }
        let estimated = HNSW_GRAPH_HEADER_LEN
            .checked_add(self.vectors.len())
            .and_then(|size| size.checked_add(node_count.checked_mul(8)?))
            .and_then(|size| size.checked_add(node_count.checked_mul(20)?))
            .ok_or_else(|| "HNSW serialized size overflow".to_string())?;
        if estimated > HNSW_MAX_GRAPH_BYTES {
            return Err("HNSW graph exceeds persisted size budget".to_string());
        }
        let mut buf = Vec::with_capacity(estimated);

        // Header
        buf.extend_from_slice(b"HNSW");
        buf.extend_from_slice(&2u32.to_le_bytes()); // version (2: neighbor distances)
        buf.push(self.metric.as_u8());
        buf.extend_from_slice(&dims_width.to_le_bytes());
        buf.extend_from_slice(&node_width.to_le_bytes());
        buf.extend_from_slice(&max_layer_width.to_le_bytes());
        buf.extend_from_slice(&self.entry_point.unwrap_or(u32::MAX).to_le_bytes());

        // Vectors (contiguous block)
        buf.extend_from_slice(&self.vectors);

        // Row IDs
        for &rid in &self.node_to_row_id {
            buf.extend_from_slice(&rid.to_le_bytes());
        }

        // Graph structure per node (version 2: includes neighbor distances)
        for (i, node) in self.nodes.iter().enumerate() {
            buf.push(self.is_deleted(i as u32) as u8);
            let layer_count = u8::try_from(node.neighbors.len())
                .map_err(|_| format!("HNSW node {i} exceeds persisted layer width"))?;
            buf.push(layer_count);
            for layer in &node.neighbors {
                let neighbor_count = u16::try_from(layer.len())
                    .map_err(|_| format!("HNSW node {i} exceeds persisted neighbor width"))?;
                buf.extend_from_slice(&neighbor_count.to_le_bytes());
                for &(nbr, dist) in layer {
                    buf.extend_from_slice(&nbr.to_le_bytes());
                    buf.extend_from_slice(&dist.to_le_bytes());
                }
            }
        }

        if buf.len() > HNSW_MAX_GRAPH_BYTES {
            return Err("HNSW graph exceeds persisted size budget".to_string());
        }
        Ok(buf)
    }

    /// Deserialize an HNSW graph from bytes.
    fn deserialize_graph(data: &[u8]) -> std::result::Result<Self, String> {
        if data.len() < HNSW_GRAPH_HEADER_LEN {
            return Err("HNSW data too short for header".to_string());
        }
        if data.len() > HNSW_MAX_GRAPH_BYTES {
            return Err("HNSW graph exceeds persisted size budget".to_string());
        }
        // Magic
        if &data[0..4] != b"HNSW" {
            return Err("Invalid HNSW magic bytes".to_string());
        }
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != 2 {
            return Err(format!("Unsupported HNSW version: {}", version));
        }
        let metric = HnswDistanceMetric::from_u8(data[8])
            .ok_or_else(|| format!("Invalid HNSW metric: {}", data[8]))?;
        let dims_bytes = u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize;
        let node_count = u32::from_le_bytes([data[13], data[14], data[15], data[16]]) as usize;
        let max_layer = u32::from_le_bytes([data[17], data[18], data[19], data[20]]) as usize;
        if dims_bytes == 0 || !dims_bytes.is_multiple_of(std::mem::size_of::<f32>()) {
            return Err("HNSW vector width is zero or not f32-aligned".to_string());
        }
        if max_layer > u8::MAX as usize {
            return Err("HNSW max layer exceeds node layer width".to_string());
        }
        let ep_raw = u32::from_le_bytes([data[21], data[22], data[23], data[24]]);
        let entry_point = if ep_raw == u32::MAX {
            None
        } else {
            Some(ep_raw)
        };

        let mut pos = HNSW_GRAPH_HEADER_LEN;

        // Vectors
        let vec_size = node_count
            .checked_mul(dims_bytes)
            .ok_or_else(|| "HNSW vector byte count overflow".to_string())?;
        let vector_end = pos
            .checked_add(vec_size)
            .ok_or_else(|| "HNSW vector end offset overflow".to_string())?;
        if vector_end > data.len() {
            return Err("HNSW data truncated at vectors".to_string());
        }
        let vectors = data[pos..vector_end].to_vec();
        pos = vector_end;

        // Row IDs
        let rid_size = node_count
            .checked_mul(8)
            .ok_or_else(|| "HNSW row ID byte count overflow".to_string())?;
        let row_ids_end = pos
            .checked_add(rid_size)
            .ok_or_else(|| "HNSW row ID end offset overflow".to_string())?;
        let minimum_node_bytes = node_count
            .checked_mul(2)
            .ok_or_else(|| "HNSW node header byte count overflow".to_string())?;
        if row_ids_end > data.len() || data.len().saturating_sub(row_ids_end) < minimum_node_bytes {
            return Err("HNSW data truncated at row_ids".to_string());
        }
        let mut node_to_row_id = Vec::with_capacity(node_count);
        let mut row_id_to_node = ahash::AHashMap::with_capacity(node_count);
        for i in 0..node_count {
            let off = pos
                .checked_add(
                    i.checked_mul(8)
                        .ok_or_else(|| "HNSW row ID offset overflow".to_string())?,
                )
                .ok_or_else(|| "HNSW row ID offset overflow".to_string())?;
            let rid = i64::from_le_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
                data[off + 4],
                data[off + 5],
                data[off + 6],
                data[off + 7],
            ]);
            if row_id_to_node.contains_key(&rid) {
                return Err(format!("HNSW graph contains duplicate row ID {rid}"));
            }
            node_to_row_id.push(rid);
            row_id_to_node.insert(rid, i as u32);
        }
        pos = row_ids_end;

        // Graph structure
        let mut nodes = Vec::with_capacity(node_count);
        let deleted_words = node_count
            .checked_add(63)
            .ok_or_else(|| "HNSW deleted bitset size overflow".to_string())?
            >> 6;
        let mut deleted_bits = vec![0u64; deleted_words];
        for node_idx in 0..node_count {
            let node_header_end = pos
                .checked_add(2)
                .ok_or_else(|| "HNSW node header offset overflow".to_string())?;
            if node_header_end > data.len() {
                return Err("HNSW data truncated at node header".to_string());
            }
            let deleted = data[pos] != 0;
            if deleted {
                deleted_bits[node_idx >> 6] |= 1u64 << (node_idx & 63);
            }
            let num_layers = data[pos + 1] as usize;
            if num_layers > max_layer.saturating_add(1) {
                return Err(format!(
                    "HNSW node {node_idx} has {num_layers} layers above max layer {max_layer}"
                ));
            }
            pos = node_header_end;

            let mut neighbors = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                let layer_header_end = pos
                    .checked_add(2)
                    .ok_or_else(|| "HNSW layer header offset overflow".to_string())?;
                if layer_header_end > data.len() {
                    return Err("HNSW data truncated at layer header".to_string());
                }
                let count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos = layer_header_end;
                let neighbor_bytes = count
                    .checked_mul(8)
                    .ok_or_else(|| "HNSW neighbor byte count overflow".to_string())?;
                let neighbor_end = pos
                    .checked_add(neighbor_bytes)
                    .ok_or_else(|| "HNSW neighbor end offset overflow".to_string())?;
                if neighbor_end > data.len() {
                    return Err("HNSW data truncated at neighbor list".to_string());
                }
                let mut layer = Vec::with_capacity(count);
                for j in 0..count {
                    let off = pos
                        .checked_add(
                            j.checked_mul(8)
                                .ok_or_else(|| "HNSW neighbor offset overflow".to_string())?,
                        )
                        .ok_or_else(|| "HNSW neighbor offset overflow".to_string())?;
                    let nid = u32::from_le_bytes([
                        data[off],
                        data[off + 1],
                        data[off + 2],
                        data[off + 3],
                    ]);
                    let dist = f32::from_le_bytes([
                        data[off + 4],
                        data[off + 5],
                        data[off + 6],
                        data[off + 7],
                    ]);
                    layer.push((nid, dist));
                }
                pos = neighbor_end;
                neighbors.push(layer);
            }
            nodes.push(HnswNode { neighbors });
        }

        if pos != data.len() {
            return Err(format!(
                "HNSW graph has {} trailing bytes",
                data.len() - pos
            ));
        }

        // Validate all neighbor IDs are within bounds — corrupt IDs would cause
        // out-of-bounds reads in unsafe pointer arithmetic during search traversal
        let node_count = nodes.len() as u32;
        for (i, node) in nodes.iter().enumerate() {
            for (l, layer) in node.neighbors.iter().enumerate() {
                for &(nid, _) in layer {
                    if nid >= node_count {
                        return Err(format!(
                            "HNSW corrupted: node {} layer {} has neighbor {} but only {} nodes exist",
                            i, l, nid, node_count
                        ));
                    }
                }
            }
        }

        // Validate entry_point is within bounds
        if let Some(ep) = entry_point {
            if ep >= node_count {
                return Err(format!(
                    "HNSW corrupted: entry_point {} but only {} nodes exist",
                    ep, node_count
                ));
            }
        }

        let inner = Self {
            nodes,
            entry_point,
            max_layer,
            vectors,
            dims_bytes,
            node_to_row_id,
            row_id_to_node,
            metric,
            scratch: SearchScratch::new(),
            deleted_bits,
            unique_map: None,
            build_seed: 0,
        };

        Ok(inner)
    }
}

/// Compute default M (max connections per node) based on vector dimensions.
///
/// Higher dimensions need more connections for good recall:
/// - dims <= 64: M=16 (standard, works well for low-dim embeddings)
/// - dims 65-255: M=32 (needed for 128d+ embeddings like SIFT, CLIP)
/// - dims >= 256: M=48 (needed for high-dim embeddings like OpenAI 1536d)
pub fn default_m_for_dims(dims: usize) -> usize {
    if dims >= 256 {
        48
    } else if dims >= 64 {
        32
    } else {
        16
    }
}

/// Adaptive ef_construction based on M.
/// Higher M needs a wider beam to find enough diverse candidates during build.
pub fn default_ef_construction(m: usize) -> usize {
    if m >= 48 {
        256
    } else if m >= 32 {
        200
    } else {
        128
    }
}

/// Adaptive ef_search based on M.
/// Higher M graphs have richer connectivity — wider search beam exploits it.
pub fn default_ef_search(m: usize) -> usize {
    if m >= 48 {
        256
    } else if m >= 32 {
        200
    } else {
        128
    }
}

/// Generate a replayable level using the standard HNSW formula
/// `floor(-ln(uniform) * ml)`. SplitMix64 turns the stable index seed and row
/// identity into a full-width deterministic sample without shared RNG state.
fn deterministic_level(build_seed: u64, row_id: i64, ml: f64) -> usize {
    let mut sample = build_seed ^ (row_id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    sample = sample.wrapping_add(0x9e37_79b9_7f4a_7c15);
    sample = (sample ^ (sample >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    sample = (sample ^ (sample >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    sample ^= sample >> 31;
    let unit = (sample >> 11) as f64 * (1.0 / ((1u64 << 53) as f64));
    let r = unit.max(f64::MIN_POSITIVE);
    (-r.ln() * ml).floor() as usize
}

fn stable_hnsw_build_seed(
    name: &str,
    table_name: &str,
    column_name: &str,
    column_id: i32,
    dims: usize,
    m: usize,
    metric: HnswDistanceMetric,
) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for bytes in [
        name.as_bytes(),
        table_name.as_bytes(),
        column_name.as_bytes(),
    ] {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    for byte in column_id
        .to_le_bytes()
        .into_iter()
        .chain(dims.to_le_bytes())
        .chain(m.to_le_bytes())
        .chain([metric.as_u8()])
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

// ─────────────────────────────────────────────────────────────
// Thread-safe free functions for parallel HNSW build
// ─────────────────────────────────────────────────────────────

#[cfg(feature = "parallel")]
/// Thread-safe search_layer operating on raw slices (no &self needed).
/// Uses bitset-based visited tracking for zero-overhead mark/check.
#[allow(clippy::too_many_arguments)]
fn search_layer_shared(
    nodes: &[HnswNode],
    deleted_bits: &[u64],
    vectors: &[u8],
    dims_bytes: usize,
    metric: HnswDistanceMetric,
    query: &[f32],
    entry_points: &[u32],
    ef: usize,
    layer: usize,
) -> Vec<MaxEntry> {
    BUILD_SCRATCH.with(|cell| {
        let mut scratch = cell.borrow_mut();
        scratch.reset(nodes.len());

        let dims = dims_bytes / 4;
        let vectors_ptr = vectors.as_ptr();
        let visited_ptr = scratch.visited.as_mut_ptr();
        let deleted_ptr = deleted_bits.as_ptr();

        for &ep in entry_points {
            if (ep as usize) >= nodes.len() {
                continue;
            }
            let n_idx = ep as usize;
            // SAFETY: visited is sized to nodes.len() bits; n_idx < nodes.len() (checked above).
            unsafe {
                *visited_ptr.add(n_idx >> 6) |= 1u64 << (n_idx & 63);
            }
            // SAFETY: n_idx < nodes.len(), so n_idx * dims_bytes is within the vectors buffer.
            let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
            // SAFETY: vectors buffer stores dims f32 values per node; pointer is aligned (system allocator).
            let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
            let d = match metric {
                HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
            };
            scratch.candidates.push(MinEntry {
                distance: d,
                node: ep,
            });
            scratch.result.push(MaxEntry {
                distance: d,
                node: ep,
            });
        }

        let mut farthest_dist = scratch.result.peek().map_or(f32::MAX, |e| e.distance);
        let mut result_len = scratch.result.len();

        let nodes_ptr = nodes.as_ptr();
        let nodes_len = nodes.len();

        while let Some(MinEntry {
            distance: c_dist,
            node: c_id,
        }) = scratch.candidates.pop()
        {
            if c_dist > farthest_dist && result_len >= ef {
                break;
            }

            // Prefetch the NEXT candidate's node struct
            if let Some(next) = scratch.candidates.peek() {
                let next_id = next.node as usize;
                if next_id < nodes_len {
                    // SAFETY: next_id < nodes_len, so the pointer is within the nodes slice.
                    let next_node_ptr = unsafe { nodes_ptr.add(next_id) } as *const u8;
                    prefetch_read(next_node_ptr);
                }
            }

            // SAFETY: c_id was inserted from a valid node index; all node indices < nodes_len.
            let node = unsafe { &*nodes_ptr.add(c_id as usize) };
            if layer < node.neighbors.len() {
                let neighbors = &node.neighbors[layer];
                let nlen = neighbors.len();
                let nptr = neighbors.as_ptr();

                let mut ni = 0usize;
                while ni < nlen {
                    // SAFETY: ni < nlen, so the pointer is within the neighbors slice.
                    let (neighbor, _) = unsafe { *nptr.add(ni) };
                    ni += 1;

                    let n_idx = neighbor as usize;

                    // Prefetch next neighbor's visited bit
                    if ni < nlen {
                        // SAFETY: ni < nlen, pointer within neighbors slice.
                        let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                        let next_v_word = next_nb >> 6;
                        // SAFETY: next_v_word < visited word count since neighbors are valid node indices.
                        prefetch_read(unsafe { visited_ptr.add(next_v_word) } as *const u8);
                    }

                    let v_word_idx = n_idx >> 6;
                    let v_bit_mask = 1u64 << (n_idx & 63);

                    // SAFETY: n_idx is a valid node index < node count; visited is sized to node count bits.
                    let v_word_ptr = unsafe { visited_ptr.add(v_word_idx) };
                    // SAFETY: v_word_ptr is valid (derived from visited_ptr with in-bounds offset above).
                    let v_word = unsafe { *v_word_ptr };
                    if (v_word & v_bit_mask) != 0 {
                        continue;
                    }
                    // SAFETY: v_word_ptr points to a valid visited word (same as the read above).
                    unsafe {
                        *v_word_ptr = v_word | v_bit_mask;
                    }

                    // SAFETY: v_word_idx < deleted_bits word count; deleted_bits covers all node indices.
                    let is_deleted = unsafe { (*deleted_ptr.add(v_word_idx)) & v_bit_mask != 0 };
                    if is_deleted {
                        continue;
                    }

                    // Prefetch next neighbor's vector data
                    if ni < nlen {
                        // SAFETY: ni < nlen, pointer within neighbors slice.
                        let next_nb = unsafe { (*nptr.add(ni)).0 } as usize;
                        // SAFETY: next_nb is a valid node index; next_nb * dims_bytes is within vectors.
                        let vec_addr = unsafe { vectors_ptr.add(next_nb * dims_bytes) };
                        prefetch_read(vec_addr);
                    }

                    // SAFETY: n_idx < node count; n_idx * dims_bytes is within the vectors buffer.
                    let v_ptr = unsafe { vectors_ptr.add(n_idx * dims_bytes) as *const f32 };
                    // SAFETY: vectors stores dims f32 values per node; pointer is aligned (system allocator).
                    let v_slice = unsafe { std::slice::from_raw_parts(v_ptr, dims) };
                    let d = match metric {
                        HnswDistanceMetric::L2 => l2_distance_sq_f32(v_slice, query),
                        HnswDistanceMetric::Cosine => cosine_distance_f32(v_slice, query),
                        HnswDistanceMetric::InnerProduct => ip_distance_f32(v_slice, query),
                    };

                    if d < farthest_dist || result_len < ef {
                        scratch.candidates.push(MinEntry {
                            distance: d,
                            node: neighbor,
                        });
                        scratch.result.push(MaxEntry {
                            distance: d,
                            node: neighbor,
                        });
                        result_len += 1;
                        if result_len > ef {
                            scratch.result.pop();
                            result_len -= 1;
                            farthest_dist = scratch.result.peek().map_or(f32::MAX, |e| e.distance);
                        } else {
                            // Keep farthest in sync while filling up to ef; otherwise break
                            // condition can become too aggressive and terminate exploration early.
                            farthest_dist = farthest_dist.max(d);
                        }
                    }
                }
            }
        }

        scratch.result.drain().collect()
    })
}

#[cfg(feature = "parallel")]
/// Thread-safe neighbor selection with diversity heuristic (free function for parallel use).
fn select_neighbors_shared(
    vectors: &[u8],
    dims_bytes: usize,
    metric: HnswDistanceMetric,
    mut candidates: Vec<MaxEntry>,
    m: usize,
) -> Vec<(u32, f32)> {
    if candidates.is_empty() || m == 0 {
        return Vec::new();
    }

    // Sort in-place (candidates is owned — no allocation)
    candidates.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));

    let mut selected: Vec<(u32, f32)> = Vec::with_capacity(m);
    let mut pruned: Vec<(u32, f32)> = Vec::with_capacity(candidates.len());

    // Phase 1: Diversity-aware selection (Algorithm 4)
    for entry in &candidates {
        if selected.len() >= m {
            break;
        }
        let dist_to_query = entry.distance;
        // Hoist entry vector lookup outside inner loop (invariant across selected neighbors)
        let start_a = entry.node as usize * dims_bytes;
        let va = as_f32_slice(&vectors[start_a..start_a + dims_bytes]);
        let mut is_diverse = true;
        for &(sel_node, _) in &selected {
            let start_b = sel_node as usize * dims_bytes;
            let vb = as_f32_slice(&vectors[start_b..start_b + dims_bytes]);
            let dist_to_selected = match metric {
                HnswDistanceMetric::L2 => l2_distance_sq_f32(va, vb),
                HnswDistanceMetric::Cosine => cosine_distance_f32(va, vb),
                HnswDistanceMetric::InnerProduct => ip_distance_f32(va, vb),
            };
            if dist_to_selected < dist_to_query {
                is_diverse = false;
                break;
            }
        }
        if is_diverse {
            selected.push((entry.node, entry.distance));
        } else {
            pruned.push((entry.node, entry.distance));
        }
    }

    // Phase 2: Fill remaining slots from pruned candidates (keepPrunedConnections)
    for entry in pruned {
        if selected.len() >= m {
            break;
        }
        selected.push(entry);
    }

    selected
}

// ─────────────────────────────────────────────────────────────
// HnswIndex — public API implementing Index trait
// ─────────────────────────────────────────────────────────────

pub struct HnswIndex {
    inner: RwLock<HnswInner>,
    name: String,
    table_name: String,
    column_ids: Vec<i32>,
    column_names: Vec<String>,
    data_types: Vec<DataType>,
    dims: usize,
    m: usize,
    m0: usize,
    ef_construction: usize,
    ef_search: usize,
    ml: f64,
    metric: HnswDistanceMetric,
    is_unique: bool,
    build_seed: u64,
}

impl HnswIndex {
    /// Create a new HNSW index
    ///
    /// # Arguments
    /// * `name` - Index name
    /// * `table_name` - Table this index belongs to
    /// * `column_name` - The vector column name
    /// * `column_id` - The vector column ID
    /// * `dims` - Vector dimensions
    /// * `m` - Max connections per node per layer (default: 16)
    /// * `ef_construction` - Build beam width (default: 200)
    /// * `ef_search` - Search beam width (default: 200)
    /// * `metric` - Distance metric (default: L2)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table_name: String,
        column_name: String,
        column_id: i32,
        dims: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: HnswDistanceMetric,
    ) -> Result<Self> {
        if name.is_empty() || table_name.is_empty() || column_name.is_empty() || column_id < 0 {
            return Err(Error::invalid_argument("invalid HNSW index identity"));
        }
        if dims == 0 || dims.checked_mul(std::mem::size_of::<f32>()).is_none() {
            return Err(Error::invalid_argument("invalid HNSW dimensions"));
        }
        if !(2..=HNSW_MAX_M).contains(&m) {
            return Err(Error::invalid_argument(format!(
                "HNSW m must be in 2..={HNSW_MAX_M}"
            )));
        }
        if ef_construction == 0
            || ef_search == 0
            || u16::try_from(ef_construction).is_err()
            || u16::try_from(ef_search).is_err()
        {
            return Err(Error::invalid_argument(
                "HNSW ef_construction/ef_search must fit non-zero u16",
            ));
        }
        let m0 = m
            .checked_mul(2)
            .ok_or_else(|| Error::invalid_argument("HNSW m0 overflow"))?;
        let ml = 1.0 / (m as f64).ln();
        let build_seed =
            stable_hnsw_build_seed(&name, &table_name, &column_name, column_id, dims, m, metric);
        Ok(Self {
            inner: RwLock::new(HnswInner::with_build_seed(dims, metric, build_seed)),
            name,
            table_name,
            column_ids: vec![column_id],
            column_names: vec![column_name],
            data_types: vec![DataType::Vector],
            dims,
            m,
            m0,
            ef_construction,
            ef_search,
            ml,
            metric,
            is_unique: false,
            build_seed,
        })
    }

    /// Set the uniqueness constraint for this HNSW index.
    /// When enabling uniqueness, builds the O(1) byte-hash lookup map.
    pub fn set_unique(&mut self, unique: bool) -> Result<()> {
        if !unique {
            self.inner.write().unique_map = None;
            self.is_unique = false;
            return Ok(());
        }

        let mut inner = self.inner.write();
        let mut exact = ahash::AHashMap::<Vec<u8>, i64>::with_capacity(inner.nodes.len());
        let mut hashed = ahash::AHashMap::<u64, Vec<i64>>::with_capacity(inner.nodes.len());
        for (node_idx, &row_id) in inner.node_to_row_id.iter().enumerate() {
            if inner.is_deleted(node_idx as u32) {
                continue;
            }
            let offset = node_idx.checked_mul(inner.dims_bytes).ok_or_else(|| {
                radixdb_core::Error::invalid_argument("HNSW vector offset overflow")
            })?;
            let end = offset.checked_add(inner.dims_bytes).ok_or_else(|| {
                radixdb_core::Error::invalid_argument("HNSW vector end offset overflow")
            })?;
            let bytes = inner.vectors.get(offset..end).ok_or_else(|| {
                radixdb_core::Error::invalid_argument("HNSW vector data truncated")
            })?;
            if let Some(existing_row_id) = exact.insert(bytes.to_vec(), row_id) {
                if existing_row_id != row_id {
                    return Err(radixdb_core::Error::unique_constraint(
                        &self.name,
                        self.column_names.join(", "),
                        format!("<vector({} dims)>", self.dims),
                    ));
                }
            }
            hashed
                .entry(HnswInner::hash_vec_bytes(bytes))
                .or_default()
                .push(row_id);
        }
        inner.unique_map = Some(hashed);
        drop(inner);
        self.is_unique = true;
        Ok(())
    }

    /// Get the distance metric used by this index
    pub fn distance_metric(&self) -> HnswDistanceMetric {
        self.metric
    }

    /// Get the HNSW tuning parameters (m, ef_construction, ef_search, metric).
    pub fn params(&self) -> (usize, usize, usize, HnswDistanceMetric) {
        (self.m, self.ef_construction, self.ef_search, self.metric)
    }

    /// Search for k nearest neighbors to query vector
    ///
    /// Returns (row_id, distance) pairs sorted by distance (ascending).
    /// Distance type depends on metric: L2=euclidean, Cosine=1-similarity, IP=-dot_product.
    pub fn search_nearest(
        &self,
        query_bytes: &[u8],
        k: usize,
        ef_search: usize,
    ) -> Vec<(i64, f64)> {
        if query_bytes.len() != self.dims * 4 {
            return Vec::new();
        }
        let inner = self.inner.read();
        inner.search(query_bytes, k, ef_search)
    }

    /// Extract vector bytes from a Value, returning the payload (without tag byte)
    fn extract_vector_bytes(value: &Value) -> Option<&[u8]> {
        match value {
            Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                Some(&data[1..])
            }
            _ => None,
        }
    }

    fn validate_vector_value<'a>(&self, values: &'a [Value]) -> Result<&'a [u8]> {
        let value = values.first().ok_or_else(|| {
            radixdb_core::Error::invalid_argument("HNSW index requires one VECTOR value")
        })?;
        let bytes = Self::extract_vector_bytes(value).ok_or_else(|| {
            radixdb_core::Error::invalid_argument("HNSW index requires a VECTOR value")
        })?;
        let got = u16::try_from(bytes.len() / 4).unwrap_or(u16::MAX);
        let expected = u16::try_from(self.dims).unwrap_or(u16::MAX);
        if bytes.len() != self.dims * 4 {
            return Err(radixdb_core::Error::VectorDimensionMismatch { expected, got });
        }
        Ok(bytes)
    }

    /// Find an exact duplicate vector in the index.
    ///
    /// This is used for UNIQUE constraint enforcement and compares raw vector bytes
    /// (not metric distance), so it works consistently across L2/Cosine/IP metrics.
    ///
    /// When the unique_map is built (is_unique=true), uses O(1) hash lookup with
    /// byte verification. Otherwise falls back to O(N) linear scan.
    fn find_exact_duplicate_in_inner(
        inner: &HnswInner,
        vec_bytes: &[u8],
        exclude_row_id: i64,
        ignored_row_ids: Option<&I64Set>,
    ) -> Option<i64> {
        // Fast path: O(1) hash lookup via unique_map
        if let Some(ref map) = inner.unique_map {
            let hash = HnswInner::hash_vec_bytes(vec_bytes);
            if let Some(row_ids) = map.get(&hash) {
                for &candidate_row_id in row_ids {
                    if candidate_row_id == exclude_row_id {
                        continue;
                    }
                    if ignored_row_ids.is_some_and(|ignored| ignored.contains(candidate_row_id)) {
                        continue;
                    }
                    // Verify byte equality (hash collision guard)
                    if let Some(&node_id) = inner.row_id_to_node.get(&candidate_row_id) {
                        if !inner.is_deleted(node_id) {
                            let offset = node_id as usize * inner.dims_bytes;
                            if let Some(existing_bytes) =
                                inner.vectors.get(offset..offset + inner.dims_bytes)
                            {
                                if existing_bytes == vec_bytes {
                                    return Some(candidate_row_id);
                                }
                            }
                        }
                    }
                }
            }
            return None;
        }

        // Slow path: O(N) linear scan (unique_map not yet built)
        for (node_idx, &existing_row_id) in inner.node_to_row_id.iter().enumerate() {
            if existing_row_id == exclude_row_id {
                continue;
            }
            if ignored_row_ids.is_some_and(|ignored| ignored.contains(existing_row_id)) {
                continue;
            }
            if inner.is_deleted(node_idx as u32) {
                continue;
            }
            let offset = node_idx * inner.dims_bytes;
            if let Some(existing_bytes) = inner.vectors.get(offset..offset + inner.dims_bytes) {
                if existing_bytes == vec_bytes {
                    return Some(existing_row_id);
                }
            }
        }
        None
    }

    /// Public exact-duplicate lookup for UNIQUE pre-validation in commit path.
    pub fn find_exact_duplicate(
        &self,
        value: &Value,
        exclude_row_id: i64,
        ignored_row_ids: Option<&I64Set>,
    ) -> Option<i64> {
        let vec_bytes = Self::extract_vector_bytes(value)?;
        if vec_bytes.len() != self.dims * 4 {
            return None;
        }
        let inner = self.inner.read();
        Self::find_exact_duplicate_in_inner(&inner, vec_bytes, exclude_row_id, ignored_row_ids)
    }

    /// Insert prepared (vec_bytes, row_id) entries using parallel or sequential path.
    fn insert_prepared(&self, inner: &mut HnswInner, prepared: &[(&[u8], i64)]) {
        #[cfg(feature = "parallel")]
        {
            inner.insert_batch_parallel(prepared, self.m, self.m0, self.ef_construction, self.ml);
        }

        #[cfg(not(feature = "parallel"))]
        {
            for &(vec_bytes, row_id) in prepared {
                inner.insert(
                    vec_bytes,
                    row_id,
                    self.m,
                    self.m0,
                    self.ef_construction,
                    self.ml,
                );
            }
        }
    }

    /// Serialize the HNSW graph to a file for snapshot persistence.
    pub fn save_graph(&self, path: &std::path::Path) -> std::io::Result<()> {
        let data = self.serialize_graph_bytes().map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        // Atomic write: temp file + rename to prevent partial graph files on crash
        let tmp_path = path.with_extension("bin.tmp");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, path)
    }

    /// Load an HNSW graph from file, returning a fully-reconstructed HnswIndex.
    /// Returns `Ok(None)` if the file does not exist.
    #[allow(clippy::too_many_arguments)]
    pub fn load_graph(
        path: &std::path::Path,
        name: String,
        table_name: String,
        column_name: String,
        column_id: i32,
        dims: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> std::io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let file_len = std::fs::metadata(path)?.len();
        if file_len > HNSW_MAX_GRAPH_BYTES as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW graph exceeds persisted size budget",
            ));
        }
        let data = std::fs::read(path)?;
        if data.len() < HNSW_IDENTITY_HEADER_LEN || &data[..4] != HNSW_IDENTITY_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing HNSW identity envelope",
            ));
        }
        let read_u32 = |offset: usize| {
            u32::from_le_bytes(data[offset..offset + 4].try_into().expect("bounded header"))
        };
        let version = read_u32(4);
        if version != HNSW_IDENTITY_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported HNSW identity version {version}"),
            ));
        }
        let name_len = read_u32(8) as usize;
        let table_len = read_u32(12) as usize;
        let column_len = read_u32(16) as usize;
        let stored_column_id = i32::from_le_bytes(data[20..24].try_into().unwrap());
        let stored_dims = read_u32(24) as usize;
        let stored_m = read_u32(28) as usize;
        let stored_ef_construction = read_u32(32) as usize;
        let stored_ef_search = read_u32(36) as usize;
        let stored_metric = HnswDistanceMetric::from_u8(data[40]).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid HNSW identity metric",
            )
        })?;
        let graph_len = u64::from_le_bytes(data[41..49].try_into().unwrap());
        let identity_len = name_len
            .checked_add(table_len)
            .and_then(|size| size.checked_add(column_len))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HNSW identity length overflow",
                )
            })?;
        let graph_start = HNSW_IDENTITY_HEADER_LEN
            .checked_add(identity_len)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HNSW identity end offset overflow",
                )
            })?;
        let graph_len = usize::try_from(graph_len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW graph length does not fit this platform",
            )
        })?;
        let graph_end = graph_start.checked_add(graph_len).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW graph end offset overflow",
            )
        })?;
        if graph_end != data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW identity envelope length mismatch",
            ));
        }
        let name_end = HNSW_IDENTITY_HEADER_LEN + name_len;
        let table_end = name_end + table_len;
        let column_end = table_end + column_len;
        let stored_name =
            std::str::from_utf8(&data[HNSW_IDENTITY_HEADER_LEN..name_end]).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HNSW name")
            })?;
        let stored_table = std::str::from_utf8(&data[name_end..table_end]).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HNSW table name")
        })?;
        let stored_column = std::str::from_utf8(&data[table_end..column_end]).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HNSW column name")
        })?;
        if stored_name != name
            || stored_table != table_name
            || stored_column != column_name
            || stored_column_id != column_id
            || stored_dims != dims
            || stored_m != m
            || stored_ef_construction != ef_construction
            || stored_ef_search != ef_search
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW graph identity does not match requested index",
            ));
        }
        match HnswInner::deserialize_graph(&data[graph_start..graph_end]) {
            Ok(mut inner) => {
                // Validate that the graph's stored dimensions match the expected
                // schema dimensions. A mismatch (e.g., from stale graph files after
                // schema evolution) would cause OOB reads in distance kernels.
                let expected_dims_bytes = dims.checked_mul(4).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "HNSW expected dimensions overflow",
                    )
                })?;
                if inner.dims_bytes != expected_dims_bytes {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "HNSW graph dimension mismatch: file has {} bytes/vector ({} dims) \
                             but schema expects {} bytes/vector ({} dims)",
                            inner.dims_bytes,
                            inner.dims_bytes / 4,
                            expected_dims_bytes,
                            dims,
                        ),
                    ));
                }
                if inner.metric != stored_metric {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "HNSW identity metric does not match graph metric",
                    ));
                }
                if !(2..=HNSW_MAX_M).contains(&m)
                    || ef_construction == 0
                    || ef_search == 0
                    || u16::try_from(ef_construction).is_err()
                    || u16::try_from(ef_search).is_err()
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid HNSW construction parameters",
                    ));
                }
                let metric = inner.metric;
                let ml = 1.0 / (m as f64).ln();
                let m0 = m.checked_mul(2).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "HNSW m0 overflow")
                })?;
                let build_seed = stable_hnsw_build_seed(
                    &name,
                    &table_name,
                    &column_name,
                    column_id,
                    dims,
                    m,
                    metric,
                );
                inner.build_seed = build_seed;
                Ok(Some(Self {
                    inner: RwLock::new(inner),
                    name,
                    table_name,
                    column_ids: vec![column_id],
                    column_names: vec![column_name],
                    data_types: vec![DataType::Vector],
                    dims,
                    m,
                    m0,
                    ef_construction,
                    ef_search,
                    ml,
                    metric,
                    is_unique: false,
                    build_seed,
                }))
            }
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }

    /// Serialize the graph to bytes (for use by snapshot code).
    pub fn serialize_graph_bytes(&self) -> Result<Vec<u8>> {
        let inner = self.inner.read();
        let graph = inner.serialize_graph().map_err(Error::invalid_argument)?;
        let name_len = u32::try_from(self.name.len())
            .map_err(|_| Error::invalid_argument("HNSW index name is too long"))?;
        let table_len = u32::try_from(self.table_name.len())
            .map_err(|_| Error::invalid_argument("HNSW table name is too long"))?;
        let column_name = &self.column_names[0];
        let column_len = u32::try_from(column_name.len())
            .map_err(|_| Error::invalid_argument("HNSW column name is too long"))?;
        let dims = u32::try_from(self.dims)
            .map_err(|_| Error::invalid_argument("HNSW dimensions exceed persisted width"))?;
        let m = u32::try_from(self.m)
            .map_err(|_| Error::invalid_argument("HNSW m exceeds persisted width"))?;
        let ef_construction = u32::try_from(self.ef_construction)
            .map_err(|_| Error::invalid_argument("HNSW ef_construction exceeds persisted width"))?;
        let ef_search = u32::try_from(self.ef_search)
            .map_err(|_| Error::invalid_argument("HNSW ef_search exceeds persisted width"))?;
        let graph_len = u64::try_from(graph.len())
            .map_err(|_| Error::invalid_argument("HNSW graph exceeds persisted width"))?;
        let capacity = HNSW_IDENTITY_HEADER_LEN
            .checked_add(self.name.len())
            .and_then(|size| size.checked_add(self.table_name.len()))
            .and_then(|size| size.checked_add(column_name.len()))
            .and_then(|size| size.checked_add(graph.len()))
            .ok_or_else(|| Error::invalid_argument("HNSW envelope size overflow"))?;
        if capacity > HNSW_MAX_GRAPH_BYTES {
            return Err(Error::invalid_argument(
                "HNSW identity envelope exceeds persisted size budget",
            ));
        }
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(HNSW_IDENTITY_MAGIC);
        encoded.extend_from_slice(&HNSW_IDENTITY_VERSION.to_le_bytes());
        encoded.extend_from_slice(&name_len.to_le_bytes());
        encoded.extend_from_slice(&table_len.to_le_bytes());
        encoded.extend_from_slice(&column_len.to_le_bytes());
        encoded.extend_from_slice(&self.column_ids[0].to_le_bytes());
        encoded.extend_from_slice(&dims.to_le_bytes());
        encoded.extend_from_slice(&m.to_le_bytes());
        encoded.extend_from_slice(&ef_construction.to_le_bytes());
        encoded.extend_from_slice(&ef_search.to_le_bytes());
        encoded.push(self.metric.as_u8());
        encoded.extend_from_slice(&graph_len.to_le_bytes());
        encoded.extend_from_slice(self.name.as_bytes());
        encoded.extend_from_slice(self.table_name.as_bytes());
        encoded.extend_from_slice(column_name.as_bytes());
        encoded.extend_from_slice(&graph);
        Ok(encoded)
    }

    /// Stable seed used to derive node levels for this index identity.
    pub fn build_seed(&self) -> u64 {
        self.build_seed
    }

    /// Returns the number of nodes in the graph (for checking if graph is non-empty).
    pub fn node_count(&self) -> usize {
        let inner = self.inner.read();
        inner.nodes.len()
    }
}

impl Index for HnswIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn build(&mut self) -> Result<()> {
        Ok(())
    }

    fn add(&self, values: &[Value], row_id: i64, _ref_id: i64) -> Result<()> {
        let vec_bytes = self.validate_vector_value(values)?;
        let mut inner = self.inner.write();
        // Enforce uniqueness using exact byte equality (metric-independent).
        if self.is_unique
            && Self::find_exact_duplicate_in_inner(&inner, vec_bytes, row_id, None).is_some()
        {
            return Err(radixdb_core::Error::unique_constraint(
                &self.name,
                self.column_names.join(", "),
                format!("<vector({} dims)>", self.dims),
            ));
        }
        inner.insert(
            vec_bytes,
            row_id,
            self.m,
            self.m0,
            self.ef_construction,
            self.ml,
        );
        Ok(())
    }

    fn add_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        let mut inner = self.inner.write();
        let dims_bytes = inner.dims_bytes;
        let expected_vec_len = self.dims * 4;

        // Validate the entire batch before mutating the graph.
        let mut prepared: Vec<(&[u8], i64)> = Vec::with_capacity(entries.len());
        for (row_id, values) in entries.iter() {
            let vec_bytes = self.validate_vector_value(values)?;
            debug_assert_eq!(vec_bytes.len(), expected_vec_len);
            prepared.push((vec_bytes, row_id));
        }

        if self.is_unique {
            // Pre-validate full batch before mutating the graph so add_batch is atomic.
            let mut seen: ahash::AHashMap<&[u8], i64> =
                ahash::AHashMap::with_capacity(prepared.len());
            for &(vec_bytes, row_id) in &prepared {
                if let Some(&existing_row_id) = seen.get(vec_bytes) {
                    if existing_row_id != row_id {
                        return Err(radixdb_core::Error::unique_constraint(
                            &self.name,
                            self.column_names.join(", "),
                            format!("<vector({} dims)>", self.dims),
                        ));
                    }
                } else {
                    seen.insert(vec_bytes, row_id);
                }

                if Self::find_exact_duplicate_in_inner(&inner, vec_bytes, row_id, None).is_some() {
                    return Err(radixdb_core::Error::unique_constraint(
                        &self.name,
                        self.column_names.join(", "),
                        format!("<vector({} dims)>", self.dims),
                    ));
                }
            }
        }

        inner.vectors.reserve(prepared.len() * dims_bytes);
        inner.nodes.reserve(prepared.len());
        inner.node_to_row_id.reserve(prepared.len());
        inner.row_id_to_node.reserve(prepared.len());

        self.insert_prepared(&mut inner, &prepared);
        Ok(())
    }

    fn remove(&self, _values: &[Value], row_id: i64, _ref_id: i64) -> Result<()> {
        let mut inner = self.inner.write();
        if let Some(&node_id) = inner.row_id_to_node.get(&row_id) {
            inner.unique_map_remove(node_id);
            inner.set_deleted(node_id);
        }
        Ok(())
    }

    fn remove_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        let mut inner = self.inner.write();
        for row_id in entries.keys() {
            if let Some(&node_id) = inner.row_id_to_node.get(&row_id) {
                inner.unique_map_remove(node_id);
                inner.set_deleted(node_id);
            }
        }
        Ok(())
    }

    fn add_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        let mut inner = self.inner.write();
        let dims_bytes = inner.dims_bytes;
        let expected_vec_len = self.dims * 4;

        // Collect valid entries for parallel path
        let mut prepared: Vec<(&[u8], i64)> = Vec::with_capacity(entries.len());
        for &(row_id, values) in entries {
            let vec_bytes = self.validate_vector_value(values)?;
            debug_assert_eq!(vec_bytes.len(), expected_vec_len);
            prepared.push((vec_bytes, row_id));
        }

        if self.is_unique {
            // Pre-validate full batch before mutating the graph so add_batch_slice is atomic.
            let mut seen: ahash::AHashMap<&[u8], i64> =
                ahash::AHashMap::with_capacity(prepared.len());
            for &(vec_bytes, row_id) in &prepared {
                if let Some(&existing_row_id) = seen.get(vec_bytes) {
                    if existing_row_id != row_id {
                        return Err(radixdb_core::Error::unique_constraint(
                            &self.name,
                            self.column_names.join(", "),
                            format!("<vector({} dims)>", self.dims),
                        ));
                    }
                } else {
                    seen.insert(vec_bytes, row_id);
                }

                if Self::find_exact_duplicate_in_inner(&inner, vec_bytes, row_id, None).is_some() {
                    return Err(radixdb_core::Error::unique_constraint(
                        &self.name,
                        self.column_names.join(", "),
                        format!("<vector({} dims)>", self.dims),
                    ));
                }
            }
        }

        inner.vectors.reserve(prepared.len() * dims_bytes);
        inner.nodes.reserve(prepared.len());
        inner.node_to_row_id.reserve(prepared.len());
        inner.row_id_to_node.reserve(prepared.len());

        self.insert_prepared(&mut inner, &prepared);
        Ok(())
    }

    fn remove_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        let mut inner = self.inner.write();
        for &(row_id, _) in entries {
            if let Some(&node_id) = inner.row_id_to_node.get(&row_id) {
                inner.unique_map_remove(node_id);
                inner.set_deleted(node_id);
            }
        }
        Ok(())
    }

    fn column_ids(&self) -> &[i32] {
        &self.column_ids
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn data_types(&self) -> &[DataType] {
        &self.data_types
    }

    fn index_type(&self) -> IndexType {
        IndexType::Hnsw
    }

    fn is_unique(&self) -> bool {
        self.is_unique
    }

    fn find(&self, _values: &[Value]) -> Result<Vec<IndexEntry>> {
        Err(radixdb_core::Error::NotSupported(
            "HNSW does not support equality lookup".to_string(),
        ))
    }

    fn find_range(
        &self,
        _min: &[Value],
        _max: &[Value],
        _min_inclusive: bool,
        _max_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        Err(radixdb_core::Error::NotSupported(
            "HNSW does not support range lookup".to_string(),
        ))
    }

    fn find_with_operator(&self, _op: Operator, _values: &[Value]) -> Result<Vec<IndexEntry>> {
        Err(radixdb_core::Error::NotSupported(
            "HNSW does not support scalar operator lookup".to_string(),
        ))
    }

    fn get_filtered_row_ids(&self, _expr: &dyn Expression) -> Result<RowIdVec> {
        Err(radixdb_core::Error::NotSupported(
            "HNSW does not support expression-based filtering".to_string(),
        ))
    }

    fn search_nearest(&self, query: &Value, k: usize, ef_search: usize) -> Option<Vec<(i64, f64)>> {
        let query_bytes = Self::extract_vector_bytes(query)?;
        if query_bytes.len() != self.dims * 4 {
            return None;
        }
        Some(self.search_nearest(query_bytes, k, ef_search))
    }

    fn hnsw_distance_metric(&self) -> Option<u8> {
        Some(self.metric.as_u8())
    }

    fn hnsw_m(&self) -> Option<u16> {
        Some(self.m as u16)
    }

    fn hnsw_ef_construction(&self) -> Option<u16> {
        Some(self.ef_construction as u16)
    }

    fn default_ef_search(&self) -> Option<usize> {
        Some(self.ef_search)
    }

    fn hnsw_graph_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.serialize_graph_bytes().map(Some)
    }

    fn hnsw_indexed_row_ids(&self) -> Option<Vec<i64>> {
        let inner = self.inner.read();
        Some(
            inner
                .node_to_row_id
                .iter()
                .enumerate()
                .filter_map(|(node_idx, &row_id)| {
                    (!inner.is_deleted(node_idx as u32)).then_some(row_id)
                })
                .collect(),
        )
    }

    fn clear(&self) {
        let mut inner = self.inner.write();
        *inner = HnswInner::with_build_seed(self.dims, self.metric, self.build_seed);
        if self.is_unique {
            inner.unique_map = Some(ahash::AHashMap::new());
        }
    }

    fn cleanup(&self) -> Result<()> {
        let mut inner = self.inner.write();
        let total = inner.nodes.len();
        if total == 0 {
            return Ok(());
        }

        // Count deleted nodes via popcount on the bitset
        let deleted: usize = inner
            .deleted_bits
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum();
        if deleted == 0 {
            return Ok(());
        }

        // Only compact when >= 20% of nodes are tombstoned
        if deleted * 5 < total {
            return Ok(());
        }

        // Collect live (vec_bytes_range, row_id) pairs
        let dims_bytes = inner.dims_bytes;
        let live_count = total - deleted;
        let mut live_entries: Vec<(usize, i64)> = Vec::with_capacity(live_count);
        for node_idx in 0..total {
            if !inner.is_deleted(node_idx as u32) {
                live_entries.push((node_idx * dims_bytes, inner.node_to_row_id[node_idx]));
            }
        }

        // Build fresh graph from live nodes only
        let mut fresh = HnswInner::with_build_seed(self.dims, self.metric, self.build_seed);
        if !live_entries.is_empty() {
            // Borrow vector data as slices for insert
            let prepared: Vec<(&[u8], i64)> = live_entries
                .iter()
                .map(|&(offset, row_id)| (&inner.vectors[offset..offset + dims_bytes], row_id))
                .collect();

            fresh.vectors.reserve(live_count * dims_bytes);
            fresh.nodes.reserve(live_count);
            fresh.node_to_row_id.reserve(live_count);
            fresh.row_id_to_node.reserve(live_count);

            #[cfg(feature = "parallel")]
            {
                fresh.insert_batch_parallel(
                    &prepared,
                    self.m,
                    self.m0,
                    self.ef_construction,
                    self.ml,
                );
            }

            #[cfg(not(feature = "parallel"))]
            {
                for &(vec_bytes, row_id) in &prepared {
                    fresh.insert(
                        vec_bytes,
                        row_id,
                        self.m,
                        self.m0,
                        self.ef_construction,
                        self.ml,
                    );
                }
            }
        }

        if self.is_unique {
            fresh.build_unique_map();
        }

        *inner = fresh;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn close(&mut self) -> Result<()> {
        Err(radixdb_core::Error::NotSupported(
            "HNSW index lifecycle is owned by its table".to_string(),
        ))
    }
}

#[cfg(test)]
#[path = "hnsw/tests.rs"]
mod tests;
