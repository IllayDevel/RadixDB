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

//! Visibility publication for overlapping and isolated segment ranges.

use super::*;

/// Recompute the visibility bitmaps for all segments in `segments`.
///
/// `seg_order` lists segment IDs in ascending (oldest-first) order.
/// Segments are processed newest-first: the first time a row_id is seen it is
/// marked visible; subsequent occurrences (in older volumes) are masked out.
/// When there is at most one segment every row is authoritative, so all
/// `visible` fields are set to `None` (fast path for the common case).
pub(super) fn compute_visibility_bitmaps(
    seg_order: &[u64],
    segments: &mut rustc_hash::FxHashMap<u64, ColdSegment>,
    reusable_seen: &mut rustc_hash::FxHashSet<i64>,
) {
    if segments.len() <= 1 {
        for cs in segments.values_mut() {
            cs.visible = None;
        }
        return;
    }
    // Reuse the caller's seen set — clear and resize, but keep the allocation.
    reusable_seen.clear();
    let total: usize = segments.values().map(|cs| cs.volume.meta.row_count).sum();
    if reusable_seen.capacity() < total {
        reusable_seen.reserve(total * 8 / 7 + 16 - reusable_seen.capacity());
    }
    // Process newest-first (seg_order is ascending, so iterate reversed)
    for &seg_id in seg_order.iter().rev() {
        if let Some(cs) = segments.get_mut(&seg_id) {
            let rc = cs.volume.meta.row_count;
            if rc == 0 {
                cs.visible = None;
                continue;
            }
            let num_words = rc.div_ceil(64);
            let mut bits = vec![!0u64; num_words];
            // Clear trailing bits beyond row_count
            let trailing = rc % 64;
            if trailing != 0 {
                bits[num_words - 1] &= (1u64 << trailing) - 1;
            }
            let mut has_overlap = false;
            for i in 0..rc {
                if !reusable_seen.insert(cs.volume.meta.row_ids.at(i)) {
                    bits[i >> 6] &= !(1u64 << (i & 63));
                    has_overlap = true;
                }
            }
            // No overlap with newer volumes: None = all visible (zero memory, no per-row check)
            cs.visible = if has_overlap {
                Some(Arc::new(bits))
            } else {
                None
            };
        }
    }
    // Shrink if capacity far exceeds what was needed. After compaction
    // merges volumes, the total row count drops but the set stays at its
    // high-water mark. Replace with a right-sized set to free the excess.
    if reusable_seen.capacity() > total * 2 + 1024 {
        *reusable_seen = rustc_hash::FxHashSet::with_capacity_and_hasher(total, Default::default());
    } else {
        reusable_seen.clear();
    }
}

/// Return true when every selected segment occupies a row-id range disjoint
/// from the rest of the published topology. New publication batches can also
/// require the selected ranges to be mutually disjoint.
///
/// Row IDs are sorted inside every volume and registration validates the
/// manifest bounds against the physical first/last IDs. Non-overlapping bounds
/// are therefore a proof that publication or replacement cannot change
/// visibility outside the selected range. This is the common append/bulk-load
/// case: retain existing visibility bitmaps and publish the new segments as
/// wholly visible without scanning the historical database.
pub(super) fn selected_ranges_are_isolated(
    selected_segment_ids: &FxHashSet<u64>,
    segments: &rustc_hash::FxHashMap<u64, ColdSegment>,
    require_internal_disjointness: bool,
) -> bool {
    let mut selected_ranges = Vec::with_capacity(selected_segment_ids.len());
    for segment_id in selected_segment_ids {
        let Some(segment) = segments.get(segment_id) else {
            return false;
        };
        if segment.volume.meta.row_count == 0 {
            continue;
        }
        let Some((minimum, maximum)) = segment
            .volume
            .meta
            .row_ids
            .first()
            .zip(segment.volume.meta.row_ids.last())
        else {
            return false;
        };
        selected_ranges.push((minimum, maximum));
    }

    selected_ranges.sort_unstable_by_key(|(minimum, maximum)| (*minimum, *maximum));
    if require_internal_disjointness
        && selected_ranges
            .windows(2)
            .any(|ranges| ranges[0].1 >= ranges[1].0)
    {
        return false;
    }

    for (segment_id, segment) in segments {
        if selected_segment_ids.contains(segment_id) || segment.volume.meta.row_count == 0 {
            continue;
        }
        let Some((minimum, maximum)) = segment
            .volume
            .meta
            .row_ids
            .first()
            .zip(segment.volume.meta.row_ids.last())
        else {
            return false;
        };
        if selected_ranges
            .iter()
            .any(|(selected_minimum, selected_maximum)| {
                *selected_minimum <= maximum && minimum <= *selected_maximum
            })
        {
            return false;
        }
    }
    true
}
