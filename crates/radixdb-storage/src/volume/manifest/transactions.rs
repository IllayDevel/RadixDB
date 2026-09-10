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

//! Transaction-private tombstones and cold-index removal journals.

use super::*;

impl SegmentManager {
    /// Add tombstone row_ids with their commit_seq (when the tombstone was created).
    /// Lock order: manifest FIRST, then tombstones (matches read paths like
    /// deduped_row_count, total_row_count, check_value_exists_in_segments).
    /// The commit_seq enables snapshot isolation: older snapshots don't see
    /// newer tombstones, so the original cold row remains visible to them.
    pub fn add_tombstones(&self, row_ids: &[i64], commit_seq: u64) {
        if row_ids.is_empty() {
            return;
        }
        let mut manifest = self.manifest.write();
        let _tombstones_guard = self.tombstones_update.lock();
        let mut new_tombstones = (*self.tombstones.load_full()).clone();
        let mut changed = false;
        for &rid in row_ids {
            use std::collections::hash_map::Entry;
            match new_tombstones.entry(rid) {
                Entry::Vacant(e) => {
                    e.insert(commit_seq);
                    manifest.tombstones.push((rid, commit_seq));
                    changed = true;
                }
                Entry::Occupied(mut e) => {
                    // Update existing tombstone if the new commit_seq is
                    // different. This ensures repeated seal-skip tombstones
                    // get a fresh sequence that won't match an older
                    // compaction snapshot.
                    if *e.get() != commit_seq {
                        let old_seq = *e.get();
                        e.insert(commit_seq);
                        // Update the manifest entry in-place.
                        if let Some(entry) = manifest
                            .tombstones
                            .iter_mut()
                            .find(|(r, s)| *r == rid && *s == old_seq)
                        {
                            entry.1 = commit_seq;
                        }
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.tombstones.store(Arc::new(new_tombstones));
            self.mark_tombstones_changed();
        }
    }

    /// Clear all tombstones (after compaction has resolved them).
    /// Lock order: manifest FIRST, then tombstones.
    pub fn clear_tombstones(&self) {
        let mut manifest = self.manifest.write();
        let _tombstones_guard = self.tombstones_update.lock();
        if manifest.tombstones.is_empty() && self.tombstones.load().is_empty() {
            return;
        }
        manifest.tombstones.clear();
        self.tombstones.store(Arc::new(FxHashMap::default()));
        self.mark_tombstones_changed();
    }

    /// Remove only tombstones that match both row_id AND commit_seq from a
    /// prior snapshot and belong to the compacted input stream. Tombstones
    /// added after the snapshot are preserved. The predicate lets compaction
    /// query immutable row-id runs directly instead of allocating a table-sized
    /// hash set of every selected row.
    pub fn remove_tombstones_matching_snapshot_where<F>(
        &self,
        snapshot: &FxHashMap<i64, u64>,
        was_compacted: F,
    ) where
        F: Fn(i64) -> bool,
    {
        if snapshot.is_empty() {
            return;
        }
        let mut manifest = self.manifest.write();
        let _tombstones_guard = self.tombstones_update.lock();
        let mut new_tombstones = (*self.tombstones.load_full()).clone();
        let before = new_tombstones.len();
        new_tombstones.retain(|rid, seq| {
            if !was_compacted(*rid) {
                return true; // not in merged volumes, keep
            }
            // Only remove if the commit_seq matches the snapshot.
            // If a newer tombstone was added (different seq), keep it.
            !matches!(snapshot.get(rid), Some(snap_seq) if *snap_seq == *seq)
        });
        if new_tombstones.len() != before {
            self.tombstones.store(Arc::new(new_tombstones));
            manifest.tombstones.retain(|&(rid, seq)| {
                if !was_compacted(rid) {
                    return true;
                }
                !matches!(snapshot.get(&rid), Some(snap_seq) if *snap_seq == seq)
            });
            self.mark_tombstones_changed();
        }
    }

    /// Capture an exact committed tombstone set only when it differs from the
    /// generation already selected by CONTROL.  The double generation read
    /// prevents pairing a newer map with an older generation during a
    /// concurrent commit; checkpoint's outer commit fence normally makes the
    /// first iteration the fast path.
    pub(crate) fn tombstone_publication_snapshot(&self) -> Option<TombstonePublicationSnapshot> {
        loop {
            let generation = self
                .tombstone_generation
                .load(std::sync::atomic::Ordering::Acquire);
            if generation
                == self
                    .durable_tombstone_generation
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return None;
            }
            let tombstones = self.tombstones.load_full();
            if generation
                == self
                    .tombstone_generation
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return Some(TombstonePublicationSnapshot {
                    generation,
                    tombstones,
                });
            }
        }
    }

    /// Confirm one exact snapshot only after its generation became reachable
    /// through CONTROL.  A later concurrent mutation remains dirty because its
    /// larger generation is not acknowledged here.
    pub(crate) fn confirm_tombstone_publication(&self, generation: u64) {
        self.durable_tombstone_generation
            .fetch_max(generation, std::sync::atomic::Ordering::AcqRel);
    }

    /// Install the complete tombstone set selected by recovery without making
    /// it appear as an unpublished runtime mutation. Recovery has no pre-open
    /// snapshots, so each descriptor high-water is a sufficient visibility
    /// point for the row IDs it owns.
    pub(crate) fn install_recovered_tombstones(&self, tombstones: FxHashMap<i64, u64>) {
        let mut entries = tombstones
            .iter()
            .map(|(&row_id, &commit_seq)| (row_id, commit_seq))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(row_id, _)| *row_id);
        self.manifest.write().tombstones = entries;
        let has_tombstones = !tombstones.is_empty();
        let _tombstones_guard = self.tombstones_update.lock();
        self.tombstones.store(Arc::new(tombstones));
        let generation = u64::from(has_tombstones);
        self.tombstone_generation
            .store(generation, std::sync::atomic::Ordering::Release);
        self.durable_tombstone_generation
            .store(generation, std::sync::atomic::Ordering::Release);
        if has_tombstones {
            self.mark_topology_changed();
        }
    }

    /// Get an Arc reference to the tombstone map. O(1) — no data clone.
    /// Use this for read-only access (membership checks, iteration).
    /// Keys are row_ids, values are commit_seq (for snapshot filtering).
    pub fn tombstone_set_arc(&self) -> Arc<FxHashMap<i64, u64>> {
        self.tombstones.load_full()
    }

    /// Check if the tombstone set is empty without cloning.
    pub fn is_tombstone_set_empty(&self) -> bool {
        self.tombstones.load().is_empty()
    }

    // ---- Per-transaction pending tombstones ----

    /// Track a cold row_id as pending tombstone for a transaction.
    /// Called during DML (UPDATE/DELETE of cold rows).
    pub fn add_pending_tombstone(&self, txn_id: i64, row_id: i64) {
        let created_at = get_fast_timestamp();
        let mut pending = self.pending_txn_tombstones.write();
        let rows = pending.entry(txn_id).or_default();
        if rows.ids.insert(row_id) {
            rows.journal.push((row_id, created_at));
        }
    }

    /// Track one statement's bounded cold-row result with a shared mutation
    /// timestamp. Existing entries retain their earlier timestamp so rollback
    /// to a later savepoint cannot resurrect a pre-savepoint tombstone.
    pub fn add_pending_tombstones(&self, txn_id: i64, row_ids: &[i64]) {
        if row_ids.is_empty() {
            return;
        }

        let created_at = get_fast_timestamp();
        let mut pending = self.pending_txn_tombstones.write();
        let rows = pending.entry(txn_id).or_default();
        for &row_id in row_ids {
            if rows.ids.insert(row_id) {
                rows.journal.push((row_id, created_at));
            }
        }
    }

    /// Get pending tombstone row_ids for a transaction (for WAL recording).
    pub fn get_pending_tombstones(&self, txn_id: i64) -> Vec<i64> {
        self.pending_txn_tombstones
            .read()
            .get(&txn_id)
            .map(|pending| pending.ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Insert pending tombstones for a transaction directly into a set (no Vec clone).
    pub fn insert_pending_tombstones_into(
        &self,
        txn_id: i64,
        dest: &mut rustc_hash::FxHashSet<i64>,
    ) {
        if let Some(pending) = self.pending_txn_tombstones.read().get(&txn_id) {
            for &id in &pending.ids {
                dest.insert(id);
            }
        }
    }

    /// Get the count of pending tombstones for a transaction without cloning.
    pub fn pending_tombstone_count(&self, txn_id: i64) -> usize {
        self.pending_txn_tombstones
            .read()
            .get(&txn_id)
            .map_or(0, |pending| pending.ids.len())
    }

    /// Check if a specific row_id is a pending tombstone for a transaction.
    /// O(1) with FxHashSet (was O(n) with Vec).
    pub fn is_pending_tombstone(&self, txn_id: i64, row_id: i64) -> bool {
        self.pending_txn_tombstones
            .read()
            .get(&txn_id)
            .is_some_and(|pending| pending.ids.contains(&row_id))
    }

    /// Commit pending tombstones: move from per-txn pending to shared tombstone set.
    /// The commit_seq is the transaction's commit sequence, used for snapshot
    /// isolation: older snapshots won't see these tombstones.
    pub fn commit_pending_tombstones(&self, txn_id: i64, commit_seq: u64) {
        let pending = self.pending_txn_tombstones.write().remove(&txn_id);
        if let Some(pending) = pending {
            if !pending.ids.is_empty() {
                let id_vec: Vec<i64> = pending.ids.into_iter().collect();
                self.add_tombstones(&id_vec, commit_seq);
            }
        }
    }

    /// Rollback pending tombstones: discard without applying.
    pub fn rollback_pending_tombstones(&self, txn_id: i64) {
        self.pending_txn_tombstones.write().remove(&txn_id);
    }

    /// Discard cold tombstones created after a savepoint while preserving
    /// earlier mutations in the same transaction.
    pub fn rollback_pending_tombstones_to_timestamp(&self, txn_id: i64, timestamp: i64) {
        let mut pending = self.pending_txn_tombstones.write();
        let remove_txn = if let Some(rows) = pending.get_mut(&txn_id) {
            while rows
                .journal
                .last()
                .is_some_and(|(_, created_at)| *created_at > timestamp)
            {
                if let Some((row_id, _)) = rows.journal.pop() {
                    rows.ids.remove(&row_id);
                }
            }
            rows.ids.is_empty()
        } else {
            false
        };
        if remove_txn {
            pending.remove(&txn_id);
        }
    }

    /// Check if a txn has any pending tombstones (for has_local_changes).
    pub fn has_pending_tombstones(&self, txn_id: i64) -> bool {
        self.pending_txn_tombstones
            .read()
            .get(&txn_id)
            .is_some_and(|pending| !pending.ids.is_empty())
    }

    /// Stage one cold-populated index removal. Shared index state is left
    /// untouched until the transaction's ordinary commit-time index transition.
    pub fn record_cold_index_removal(
        &self,
        txn_id: i64,
        index: Arc<dyn Index>,
        values: Vec<Value>,
        row_id: i64,
    ) {
        self.pending_txn_index_removals
            .lock()
            .entry(txn_id)
            .or_default()
            .push(PendingColdIndexRemoval {
                index,
                values,
                row_id,
                removed_at: get_fast_timestamp(),
            });
    }

    pub fn mark_cold_populated_index(&self, name: &str) {
        self.cold_populated_indexes
            .write()
            .insert(SmartString::from(name));
    }

    pub fn unmark_cold_populated_index(&self, name: &str) {
        self.cold_populated_indexes.write().remove(name);
    }

    pub fn rename_cold_populated_index(&self, old_name: &str, new_name: &str) {
        let mut indexes = self.cold_populated_indexes.write();
        if indexes.remove(old_name) {
            indexes.insert(SmartString::from(new_name));
        }
    }

    pub fn is_cold_populated_index(&self, name: &str) -> bool {
        self.cold_populated_indexes.read().contains(name)
    }

    pub fn cold_populated_index_names(&self) -> FxHashSet<SmartString> {
        self.cold_populated_indexes.read().clone()
    }

    pub fn has_pending_cold_index_removals(&self, txn_id: i64) -> bool {
        self.pending_txn_index_removals
            .lock()
            .get(&txn_id)
            .is_some_and(|entries| !entries.is_empty())
    }

    pub fn cold_index_removal_checkpoint(&self, txn_id: i64) -> usize {
        self.pending_txn_index_removals
            .lock()
            .get(&txn_id)
            .map_or(0, Vec::len)
    }

    pub fn rollback_cold_index_removals_to_checkpoint(&self, txn_id: i64, checkpoint: usize) {
        let mut pending = self.pending_txn_index_removals.lock();
        let Some(entries) = pending.get_mut(&txn_id) else {
            return;
        };
        entries.truncate(checkpoint.min(entries.len()));
        if entries.is_empty() {
            pending.remove(&txn_id);
        }
    }

    pub fn pending_cold_index_removals(
        &self,
        txn_id: i64,
    ) -> Vec<(Arc<dyn Index>, Vec<Value>, i64)> {
        self.pending_txn_index_removals
            .lock()
            .get(&txn_id)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| (Arc::clone(&entry.index), entry.values.clone(), entry.row_id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The commit-time MVCC index transition consumed the staged removals.
    pub fn commit_cold_index_removals(&self, txn_id: i64) {
        self.pending_txn_index_removals.lock().remove(&txn_id);
    }

    /// Discard transaction-private removals; shared indexes were never changed.
    pub fn rollback_cold_index_removals(&self, txn_id: i64) {
        self.pending_txn_index_removals.lock().remove(&txn_id);
    }

    /// Discard removals newer than a savepoint timestamp.
    pub fn rollback_cold_index_removals_to_timestamp(&self, txn_id: i64, timestamp: i64) {
        let mut pending = self.pending_txn_index_removals.lock();
        let Some(entries) = pending.get_mut(&txn_id) else {
            return;
        };
        let keep = entries.partition_point(|entry| entry.removed_at <= timestamp);
        entries.truncate(keep);
        if entries.is_empty() {
            pending.remove(&txn_id);
        }
    }
}
