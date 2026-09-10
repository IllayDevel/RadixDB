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

//! Atomic manifest publication, recovery selection, and segment replacement.

use super::*;

impl SegmentManager {
    /// Replace only immutable INDEX attachments while preserving DATA
    /// identity, row topology, schema mappings and visibility bitmaps.
    /// Physical DDL publication calls this after CONTROL is durable so new
    /// readers immediately use the same accelerator generation as recovery.
    pub fn attach_index_artifacts(
        &self,
        replacements: &[(crate::v6::ArtifactRef, Arc<FrozenVolume>)],
    ) -> Result<()> {
        if replacements.is_empty() {
            return Ok(());
        }
        let manifest = self.manifest.read();
        let _segments_guard = self.segments_update.lock();
        let published = self.segments.load_full();
        if manifest.segments.len() != published.len() {
            return Err(Error::internal(format!(
                "cannot attach INDEX artifacts for '{}': runtime topology is incoherent",
                self.table_name()
            )));
        }
        let mut seen = FxHashSet::default();
        let mut prepared = Vec::with_capacity(replacements.len());
        for (data_reference, volume) in replacements {
            if !seen.insert(*data_reference) {
                return Err(Error::internal(
                    "INDEX attachment repeats one DATA artifact",
                ));
            }
            if volume.artifact_index_source().is_none()
                || volume
                    .artifact_source()
                    .is_none_or(|source| source.layout().reference() != *data_reference)
            {
                return Err(Error::internal(
                    "INDEX attachment is not bound to the requested DATA artifact",
                ));
            }
            let matches = published
                .iter()
                .filter(|(_, segment)| {
                    segment
                        .volume
                        .artifact_source()
                        .is_some_and(|source| source.layout().reference() == *data_reference)
                })
                .collect::<Vec<_>>();
            let [(segment_id, current)] = matches.as_slice() else {
                return Err(Error::internal(format!(
                    "DATA artifact '{}' does not identify exactly one runtime segment",
                    data_reference.id()
                )));
            };
            let replacement = ColdSegment::new(
                Arc::clone(volume),
                current.mapping.clone(),
                current.schema_version,
                current.visible.clone(),
            )?;
            prepared.push((**segment_id, replacement));
        }

        let mut next = (*published).clone();
        for (segment_id, replacement) in prepared {
            next.insert(segment_id, replacement);
        }
        self.segments.store(Arc::new(next));
        drop(manifest);
        self.mark_topology_changed();
        Ok(())
    }

    /// Reserve one process-local segment identity for the in-memory topology.
    ///
    /// Durable segment identity and allocator state belong exclusively to the
    /// CONTROL-selected artifact generation. A failed publication may leave a
    /// harmless runtime gap; restart reconstructs a fresh monotonic runtime
    /// range from the selected table manifest.
    pub fn reserve_runtime_segment_id(&self) -> u64 {
        self.manifest_mut().allocate_segment_id()
    }

    /// Restore the durable allocator floor after recovery. Tombstone
    /// descriptors consume physical segment sequence numbers even though they
    /// are not installed as scannable runtime row segments.
    pub(crate) fn ensure_runtime_segment_sequence(&self, next_segment_id: u64) {
        let mut manifest = self.manifest.write();
        manifest.next_segment_id = manifest.next_segment_id.max(next_segment_id);
    }

    /// Record a column rename. The caller must call invalidate_mappings()
    /// afterwards to recompute column mappings with the new rename.
    pub fn record_column_rename(&self, old_name: &str, new_name: &str) {
        // Persist in manifest for restart
        self.manifest
            .write()
            .column_renames
            .push((SmartString::from(old_name), SmartString::from(new_name)));
    }

    /// Remove all segments and tombstones (for DROP TABLE / TRUNCATE).
    pub fn clear(&self) {
        let had_tombstones = !self.tombstones.load().is_empty();
        {
            let mut manifest = self.manifest.write();
            manifest.segments.clear();
            manifest.tombstones.clear();
        }
        {
            let _segments_guard = self.segments_update.lock();
            self.segments.store(Arc::new(FxHashMap::default()));
        }
        {
            let _tombstones_guard = self.tombstones_update.lock();
            self.tombstones.store(Arc::new(FxHashMap::default()));
        }
        self.mark_segment_topology_changed();
        if had_tombstones {
            self.mark_tombstones_changed();
        }
        self.has_segments_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Publish an empty runtime segment set for TRUNCATE. Durable membership is
    /// owned by the database generation publisher and unreachable artifacts are
    /// reclaimed only by generation-aware GC.
    pub fn truncate_persisted_segments(&self) -> Result<()> {
        self.clear();
        Ok(())
    }

    /// Atomically replace old segments with a new compacted segment.
    /// Both operations happen under manifest write + segment CoW writer mutex,
    /// so concurrent queries see either the old Arc snapshot or the new one,
    /// never an in-place intermediate state.
    pub fn replace_segments_atomic(
        &self,
        new_segment_id: u64,
        new_volume: Arc<FrozenVolume>,
        new_meta: SegmentMeta,
        old_segment_ids: &[u64],
    ) -> Result<()> {
        self.replace_segments_atomic_guarded(
            new_segment_id,
            new_volume,
            new_meta,
            old_segment_ids,
            ReplacementExpectation::Unchecked,
            None,
        )
    }

    pub(super) fn replace_segments_atomic_guarded(
        &self,
        new_segment_id: u64,
        new_volume: Arc<FrozenVolume>,
        new_meta: SegmentMeta,
        old_segment_ids: &[u64],
        expectation: ReplacementExpectation<'_>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        let mut registration = SegmentRegistration::new(new_segment_id, new_volume, new_meta);
        self.normalize_new_segment_path(&mut registration.meta)?;
        self.validate_segment_before_register(&registration, expected_schema_version)?;
        if let ReplacementExpectation::Compaction(token) = expectation {
            if registration.meta.level != token.target_level() {
                return Err(Error::internal(format!(
                    "refusing compaction output segment {} for {}: level {:?} does not match target {:?}",
                    new_segment_id,
                    self.table_name(),
                    registration.meta.level,
                    token.target_level()
                )));
            }
        }
        // Atomic for readers: segment map is published as one ArcSwap store.
        // Bitmap computation runs before publish; segment writers are serialized.
        {
            let mut manifest = self.manifest.write();
            self.validate_replacement_snapshot(
                &manifest,
                old_segment_ids,
                &[new_segment_id],
                expectation,
                expected_schema_version,
            )?;
            let insert_pos = manifest
                .segments
                .iter()
                .position(|s| old_segment_ids.contains(&s.segment_id))
                .unwrap_or(manifest.segments.len());
            manifest.remove_segments(old_segment_ids);
            if new_segment_id >= manifest.next_segment_id {
                manifest.next_segment_id = new_segment_id + 1;
            }
            let insert_pos = insert_pos.min(manifest.segments.len());
            let seg_schema_version = registration.meta.schema_version;
            manifest.segments.insert(insert_pos, registration.meta);

            let cold =
                ColdSegment::new_identity_mapping(registration.volume, seg_schema_version, None)?;
            let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
            let _segments_guard = self.segments_update.lock();
            let published = self.segments.load_full();
            let old_ids = old_segment_ids.iter().copied().collect::<FxHashSet<_>>();
            let old_ranges_are_isolated =
                selected_ranges_are_isolated(&old_ids, published.as_ref(), false);
            let mut new_map = (*published).clone();
            for &id in old_segment_ids {
                new_map.remove(&id);
            }
            new_map.insert(new_segment_id, cold);
            let new_ids = std::iter::once(new_segment_id).collect::<FxHashSet<_>>();
            if old_ranges_are_isolated && selected_ranges_are_isolated(&new_ids, &new_map, true) {
                if let Some(segment) = new_map.get_mut(&new_segment_id) {
                    segment.visible = None;
                }
            } else {
                compute_visibility_bitmaps(
                    &seg_ids,
                    &mut new_map,
                    &mut self.visibility_seen.lock(),
                );
            }
            self.segments.store(Arc::new(new_map));
        }
        self.mark_segment_topology_changed();
        Ok(())
    }

    /// Atomically replace old segments with multiple new ones.
    /// Used by compaction-with-split when the merged output exceeds target_volume_rows.
    pub fn replace_segments_atomic_multi(
        &self,
        new_volumes: Vec<(u64, Arc<FrozenVolume>, SegmentMeta)>,
        old_segment_ids: &[u64],
    ) -> Result<()> {
        self.replace_segments_atomic_multi_guarded(
            new_volumes,
            old_segment_ids,
            ReplacementExpectation::Unchecked,
            None,
        )
    }

    #[cfg(test)]
    pub fn replace_segments_atomic_multi_checked(
        &self,
        new_volumes: Vec<(u64, Arc<FrozenVolume>, SegmentMeta)>,
        old_segment_ids: &[u64],
        expected_segment_generation: Option<u64>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        let expectation = expected_segment_generation.map_or(
            ReplacementExpectation::Unchecked,
            ReplacementExpectation::SegmentGeneration,
        );
        self.replace_segments_atomic_multi_guarded(
            new_volumes,
            old_segment_ids,
            expectation,
            expected_schema_version,
        )
    }

    pub fn replace_segments_atomic_multi_compaction_checked(
        &self,
        new_volumes: Vec<(u64, Arc<FrozenVolume>, SegmentMeta)>,
        token: &CompactionToken,
        expected_schema_version: u64,
    ) -> Result<()> {
        let old_segment_ids = token.input_ids().collect::<Vec<_>>();
        self.replace_segments_atomic_multi_guarded(
            new_volumes,
            &old_segment_ids,
            ReplacementExpectation::Compaction(token),
            Some(expected_schema_version),
        )
    }

    pub(super) fn replace_segments_atomic_multi_guarded(
        &self,
        mut new_volumes: Vec<(u64, Arc<FrozenVolume>, SegmentMeta)>,
        old_segment_ids: &[u64],
        expectation: ReplacementExpectation<'_>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        if new_volumes.is_empty() {
            return self.replace_segments_atomic_remove_only_guarded(
                old_segment_ids,
                expectation,
                expected_schema_version,
            );
        }
        if new_volumes.len() == 1 {
            let (id, vol, meta) = new_volumes.into_iter().next().unwrap();
            return self.replace_segments_atomic_guarded(
                id,
                vol,
                meta,
                old_segment_ids,
                expectation,
                expected_schema_version,
            );
        }
        let mut batch_ids = FxHashSet::default();
        for (segment_id, volume, meta) in &mut new_volumes {
            self.normalize_new_segment_path(meta)?;
            let registration =
                SegmentRegistration::new(*segment_id, Arc::clone(volume), meta.clone());
            self.validate_segment_before_register(&registration, expected_schema_version)?;
            if let ReplacementExpectation::Compaction(token) = expectation {
                if meta.level != token.target_level() {
                    return Err(Error::internal(format!(
                        "refusing compaction output segment {} for {}: level {:?} does not match target {:?}",
                        segment_id,
                        self.table_name(),
                        meta.level,
                        token.target_level()
                    )));
                }
            }
            if !batch_ids.insert(*segment_id) {
                return Err(Error::internal(format!(
                    "refusing to publish duplicate replacement segment {} for {}",
                    segment_id,
                    self.table_name()
                )));
            }
        }
        {
            let mut manifest = self.manifest.write();
            self.validate_replacement_snapshot(
                &manifest,
                old_segment_ids,
                &batch_ids.iter().copied().collect::<Vec<_>>(),
                expectation,
                expected_schema_version,
            )?;
            let insert_pos = manifest
                .segments
                .iter()
                .position(|s| old_segment_ids.contains(&s.segment_id))
                .unwrap_or(manifest.segments.len());
            manifest.remove_segments(old_segment_ids);

            let _segments_guard = self.segments_update.lock();
            let published = self.segments.load_full();
            let old_ids = old_segment_ids.iter().copied().collect::<FxHashSet<_>>();
            let old_ranges_are_isolated =
                selected_ranges_are_isolated(&old_ids, published.as_ref(), false);
            let mut new_map = (*published).clone();
            for &id in old_segment_ids {
                new_map.remove(&id);
            }

            let insert_pos = insert_pos.min(manifest.segments.len());
            for (i, (seg_id, vol, meta)) in new_volumes.into_iter().enumerate() {
                if seg_id >= manifest.next_segment_id {
                    manifest.next_segment_id = seg_id + 1;
                }
                let seg_schema_version = meta.schema_version;
                manifest.segments.insert(insert_pos + i, meta);

                let cold = ColdSegment::new_identity_mapping(vol, seg_schema_version, None)?;
                new_map.insert(seg_id, cold);
            }

            let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
            if old_ranges_are_isolated && selected_ranges_are_isolated(&batch_ids, &new_map, true) {
                for segment_id in &batch_ids {
                    if let Some(segment) = new_map.get_mut(segment_id) {
                        segment.visible = None;
                    }
                }
            } else {
                compute_visibility_bitmaps(
                    &seg_ids,
                    &mut new_map,
                    &mut self.visibility_seen.lock(),
                );
            }
            self.segments.store(Arc::new(new_map));
        }
        self.has_segments_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.mark_segment_topology_changed();
        Ok(())
    }

    pub fn replace_segments_atomic_remove_only_compaction_checked(
        &self,
        token: &CompactionToken,
        expected_schema_version: u64,
    ) -> Result<()> {
        let old_segment_ids = token.input_ids().collect::<Vec<_>>();
        self.replace_segments_atomic_remove_only_guarded(
            &old_segment_ids,
            ReplacementExpectation::Compaction(token),
            Some(expected_schema_version),
        )
    }

    /// Revalidate a planned compaction without mutating the manifest. Long
    /// jobs call this at cooperative yield points so incompatible DDL or input
    /// replacement stops work before another output/index is built.
    pub fn validate_compaction_token_live(
        &self,
        token: &CompactionToken,
        current_schema_epoch: u64,
    ) -> Result<()> {
        let manifest = self.manifest.read();
        self.validate_compaction_snapshot(&manifest, token, Some(current_schema_epoch))
    }

    pub(super) fn replace_segments_atomic_remove_only_guarded(
        &self,
        old_segment_ids: &[u64],
        expectation: ReplacementExpectation<'_>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        {
            let mut manifest = self.manifest.write();
            self.validate_replacement_snapshot(
                &manifest,
                old_segment_ids,
                &[],
                expectation,
                expected_schema_version,
            )?;
            manifest.remove_segments(old_segment_ids);
            let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
            let _segments_guard = self.segments_update.lock();
            let published = self.segments.load_full();
            let old_ids = old_segment_ids.iter().copied().collect::<FxHashSet<_>>();
            let old_ranges_are_isolated =
                selected_ranges_are_isolated(&old_ids, published.as_ref(), false);
            let mut new_map = (*published).clone();
            for &id in old_segment_ids {
                new_map.remove(&id);
            }
            let has_any = !new_map.is_empty();
            if !old_ranges_are_isolated {
                compute_visibility_bitmaps(
                    &seg_ids,
                    &mut new_map,
                    &mut self.visibility_seen.lock(),
                );
            }
            self.segments.store(Arc::new(new_map));
            self.has_segments_flag
                .store(has_any, std::sync::atomic::Ordering::Relaxed);
        }
        self.mark_segment_topology_changed();
        Ok(())
    }

    pub(super) fn validate_replacement_snapshot(
        &self,
        manifest: &TableManifest,
        old_segment_ids: &[u64],
        new_segment_ids: &[u64],
        expectation: ReplacementExpectation<'_>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        match expectation {
            ReplacementExpectation::Unchecked => {}
            #[cfg(test)]
            ReplacementExpectation::SegmentGeneration(expected)
                if self.segment_generation() != expected =>
            {
                return Err(Error::internal(format!(
                    "refusing stale segment publication for {}: generation changed",
                    self.table_name()
                )));
            }
            #[cfg(test)]
            ReplacementExpectation::SegmentGeneration(_) => {}
            ReplacementExpectation::Compaction(token) => {
                self.validate_compaction_snapshot(manifest, token, expected_schema_version)?;
            }
        }
        let current_ids: FxHashSet<u64> = manifest
            .segments
            .iter()
            .map(|meta| meta.segment_id)
            .collect();
        if old_segment_ids.iter().any(|id| !current_ids.contains(id)) {
            return Err(Error::internal(format!(
                "refusing topology publication for {}: replacement input is no longer live",
                self.table_name()
            )));
        }
        if new_segment_ids
            .iter()
            .any(|id| current_ids.contains(id) && !old_segment_ids.contains(id))
        {
            return Err(Error::internal(format!(
                "refusing topology publication for {}: replacement segment id is already live",
                self.table_name()
            )));
        }
        Ok(())
    }

    pub(super) fn validate_compaction_snapshot(
        &self,
        manifest: &TableManifest,
        token: &CompactionToken,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        if token.table_name.as_str() != self.table_name() {
            return Err(Error::internal(
                "refusing compaction publication for a different table",
            ));
        }
        if expected_schema_version != Some(token.schema_epoch()) {
            return Err(Error::internal(format!(
                "refusing compaction publication for {}: schema epoch changed from {} to {:?}",
                self.table_name(),
                token.schema_epoch(),
                expected_schema_version
            )));
        }

        let published = self.segments.load();
        let mut previous_position = None;
        for expected in &token.inputs {
            let position = manifest
                .segments
                .iter()
                .position(|meta| meta.segment_id == expected.segment_id)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "refusing compaction publication for {}: input segment {} is no longer live",
                        self.table_name(),
                        expected.segment_id
                    ))
                })?;
            if previous_position.is_some_and(|previous| position != previous + 1) {
                return Err(Error::internal(format!(
                    "refusing compaction publication for {}: input manifest range changed",
                    self.table_name()
                )));
            }
            previous_position = Some(position);
            let cold = published.get(&expected.segment_id).ok_or_else(|| {
                Error::internal(format!(
                    "refusing compaction publication for {}: input segment {} has no published volume",
                    self.table_name(),
                    expected.segment_id
                ))
            })?;
            let current = Self::effective_compaction_input(&manifest.segments[position], cold)?;
            if current != *expected {
                return Err(Error::internal(format!(
                    "refusing compaction publication for {}: input segment {} identity changed",
                    self.table_name(),
                    expected.segment_id
                )));
            }
        }
        Ok(())
    }

    /// Get the manifest for reading (e.g., to iterate segment metadata).
    pub fn manifest(&self) -> parking_lot::RwLockReadGuard<'_, TableManifest> {
        self.manifest.read()
    }

    /// Raw CoW snapshot. Callers that only need metadata avoid reading blocks.
    pub fn segments_raw(&self) -> Arc<FxHashMap<u64, ColdSegment>> {
        self.segments.load_full()
    }

    /// Get the manifest for writing (e.g., to allocate segment IDs).
    pub fn manifest_mut(&self) -> parking_lot::RwLockWriteGuard<'_, TableManifest> {
        self.manifest.write()
    }

    /// Get the volume directory path.
    pub fn volume_dir(&self) -> Option<&Path> {
        self.volume_dir.as_deref()
    }

    /// Recompute visibility bitmaps for all segments.
    /// Called after a batch of volume registrations during recovery so that the
    /// bitmaps reflect the final set of loaded volumes.
    pub fn recompute_visibility(&self) {
        let manifest = self.manifest.read();
        let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
        drop(manifest);
        let _segments_guard = self.segments_update.lock();
        let mut new_map = (*self.segments.load_full()).clone();
        compute_visibility_bitmaps(&seg_ids, &mut new_map, &mut self.visibility_seen.lock());
        self.segments.store(Arc::new(new_map));
        self.mark_topology_changed();
    }
}

impl std::fmt::Debug for SegmentManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let manifest = self.manifest.read();
        f.debug_struct("SegmentManager")
            .field("table", &self.table_name())
            .field("segments", &manifest.segments.len())
            .field("next_id", &manifest.next_segment_id)
            .field("tombstones", &self.tombstones.load().len())
            .finish()
    }
}
