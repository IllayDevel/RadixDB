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

//! Segment topology, registration, compaction inputs, cache pressure, and renames.

use super::*;

impl SegmentManager {
    /// Get the number of segments.
    pub fn segment_count(&self) -> usize {
        self.manifest.read().segments.len()
    }

    /// Capture exact immutable inputs for one compaction job from an already
    /// coherent manifest/segment snapshot. This method performs no I/O and
    /// does not acquire another manifest lock.
    pub fn capture_compaction_token_from_snapshot(
        &self,
        manifest: &TableManifest,
        published: &FxHashMap<u64, ColdSegment>,
        input_ids: &[u64],
        schema_epoch: u64,
        tombstone_boundary: Option<u64>,
        target_level: SegmentLevel,
    ) -> Result<CompactionToken> {
        if manifest.table_name.as_str() != self.table_name() {
            return Err(Error::internal(
                "compaction snapshot belongs to a different table",
            ));
        }
        if input_ids.is_empty() {
            return Err(Error::internal("compaction token has no inputs"));
        }

        let mut inputs = Vec::with_capacity(input_ids.len());
        let mut seen = FxHashSet::default();
        let mut previous_position = None;
        for &segment_id in input_ids {
            if !seen.insert(segment_id) {
                return Err(Error::internal(format!(
                    "compaction token repeats input segment {segment_id}"
                )));
            }
            let position = manifest
                .segments
                .iter()
                .position(|meta| meta.segment_id == segment_id)
                .ok_or_else(|| {
                    Error::internal(format!("compaction input segment {segment_id} is not live"))
                })?;
            if previous_position.is_some_and(|previous| position != previous + 1) {
                return Err(Error::internal(
                    "compaction inputs are not one contiguous manifest range",
                ));
            }
            previous_position = Some(position);
            let cold = published.get(&segment_id).ok_or_else(|| {
                Error::internal(format!(
                    "compaction input segment {segment_id} has no published volume"
                ))
            })?;
            inputs.push(Self::effective_compaction_input(
                &manifest.segments[position],
                cold,
            )?);
        }

        Ok(CompactionToken {
            table_name: self.table_name(),
            schema_epoch,
            inputs,
            tombstone_boundary,
            target_level,
        })
    }

    pub(super) fn effective_compaction_input(
        meta: &SegmentMeta,
        cold: &ColdSegment,
    ) -> Result<SegmentMeta> {
        let source = cold.volume.artifact_source().ok_or_else(|| {
            Error::internal(format!(
                "compaction input segment {} has no canonical DATA source",
                meta.segment_id
            ))
        })?;
        let reference = source.layout().reference();
        if reference.relative_path() != meta.file_path {
            return Err(Error::internal(format!(
                "compaction input segment {} DATA path disagrees with runtime metadata",
                meta.segment_id
            )));
        }
        if usize::try_from(source.layout().header().row_count()).ok() != Some(meta.row_count) {
            return Err(Error::internal(format!(
                "compaction input segment {} DATA row count disagrees with runtime metadata",
                meta.segment_id
            )));
        }
        Ok(meta.clone())
    }

    /// Exact current L0/legacy debt used for write admission. This observes
    /// immutable descriptors only and performs no payload I/O.
    pub fn l0_debt_snapshot(&self) -> L0DebtSnapshot {
        let published = self.segments.load();
        let manifest = self.manifest.read();
        let mut debt = L0DebtSnapshot::default();
        for meta in &manifest.segments {
            if !matches!(meta.level, SegmentLevel::Unleveled | SegmentLevel::L0) {
                continue;
            }
            debt.segments = debt.segments.saturating_add(1);
            let data_bytes = published
                .get(&meta.segment_id)
                .and_then(|cold| cold.volume.artifact_source())
                .map_or(0, |source| source.layout().reference().byte_length());
            let index_bytes = published
                .get(&meta.segment_id)
                .and_then(|cold| cold.volume.artifact_index_source())
                .map_or(0, |source| source.layout().reference().byte_length());
            debt.physical_bytes = debt
                .physical_bytes
                .saturating_add(data_bytes.saturating_add(index_bytes));
        }
        debt
    }

    /// Return a non-blocking, bounded aggregate of published cold ownership.
    ///
    /// The ArcSwap snapshots are immutable for the duration of this method,
    /// so no seal/compaction lock is acquired. The visit budget gives the
    /// runtime diagnostics endpoint a structural upper bound even for a
    /// pathological catalog with very many small segments.
    pub fn runtime_owner_snapshot(&self, max_segments: usize) -> SegmentRuntimeOwnerSnapshot {
        let published = self.segments.load();
        let mut snapshot = SegmentRuntimeOwnerSnapshot {
            segments: published.len() as u64,
            tombstones: self.tombstones.load().len() as u64,
            truncated: published.len() > max_segments,
            ..SegmentRuntimeOwnerSnapshot::default()
        };

        if let Some(manifest) = self.manifest.try_read() {
            for segment in manifest.segments.iter().take(max_segments) {
                match segment.level {
                    SegmentLevel::Unleveled => {
                        snapshot.unleveled_segments = snapshot.unleveled_segments.saturating_add(1);
                    }
                    SegmentLevel::L0 => {
                        snapshot.l0_segments = snapshot.l0_segments.saturating_add(1);
                    }
                    SegmentLevel::L1 => {
                        snapshot.l1_segments = snapshot.l1_segments.saturating_add(1);
                    }
                }
                if matches!(segment.level, SegmentLevel::Unleveled | SegmentLevel::L0) {
                    let data_bytes = published
                        .get(&segment.segment_id)
                        .and_then(|cold| cold.volume.artifact_source())
                        .map_or(0, |source| source.layout().reference().byte_length());
                    let index_bytes = published
                        .get(&segment.segment_id)
                        .and_then(|cold| cold.volume.artifact_index_source())
                        .map_or(0, |source| source.layout().reference().byte_length());
                    snapshot.l0_debt_physical_bytes = snapshot
                        .l0_debt_physical_bytes
                        .saturating_add(data_bytes.saturating_add(index_bytes));
                }
            }
            snapshot.truncated |= manifest.segments.len() > max_segments;
        } else {
            snapshot.level_metadata_busy = true;
        }

        for segment in published.values().take(max_segments) {
            let volume = &segment.volume;
            let resident = volume.resident_memory();
            snapshot.rows = snapshot
                .rows
                .saturating_add(volume.meta.row_ids.len() as u64);
            snapshot.resident_bytes = snapshot
                .resident_bytes
                .saturating_add(resident.total() as u64);
            snapshot.metadata_bytes = snapshot
                .metadata_bytes
                .saturating_add(resident.metadata as u64);
            snapshot.row_id_bytes = snapshot
                .row_id_bytes
                .saturating_add(resident.row_ids as u64);
            snapshot.exact_index_bytes = snapshot
                .exact_index_bytes
                .saturating_add(resident.exact_indices as u64);
            snapshot.ordered_index_bytes = snapshot
                .ordered_index_bytes
                .saturating_add(resident.ordered_indices as u64);
            snapshot.descriptor_bytes = snapshot
                .descriptor_bytes
                .saturating_add(resident.descriptor.saturating_add(resident.block_source) as u64);
            snapshot.column_payload_bytes = snapshot
                .column_payload_bytes
                .saturating_add(resident.column_payload as u64);
        }
        snapshot
    }

    /// Get the number of committed tombstones.
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.load().len()
    }

    /// Get the maximum row_count across all segments.
    pub fn max_segment_row_count(&self) -> usize {
        let manifest = self.manifest.read();
        manifest
            .segments
            .iter()
            .map(|s| s.row_count)
            .max()
            .unwrap_or(0)
    }

    /// Per-volume statistics for PRAGMA VOLUME_STATS.
    /// Returns total resident ownership plus its disjoint components for each volume.
    #[allow(clippy::type_complexity)]
    pub fn volume_stats(
        &self,
    ) -> Vec<(
        u64,
        &'static str,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        u64,
    )> {
        let current_epoch = self
            .current_eviction_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let manifest = self.manifest.read();
        let segments = self.segments.load_full();
        let mut stats = Vec::with_capacity(manifest.segments.len());
        for meta in &manifest.segments {
            let seg_id = meta.segment_id;
            if let Some(cs) = segments.get(&seg_id) {
                let vol = &cs.volume;
                let tier = if vol.columns.is_eager() {
                    "hot"
                } else {
                    "cold"
                };
                let row_count = vol.meta.row_ids.len();
                let resident = vol.resident_memory();
                let memory_bytes = resident.total();
                let last_epoch = vol
                    .last_access_epoch
                    .load(std::sync::atomic::Ordering::Relaxed);
                let idle_cycles = if last_epoch == u64::MAX || current_epoch == 0 {
                    0
                } else {
                    current_epoch.saturating_sub(last_epoch)
                };
                stats.push((
                    seg_id,
                    tier,
                    row_count,
                    memory_bytes,
                    resident.metadata,
                    resident.row_ids,
                    resident.exact_indices,
                    resident.ordered_indices,
                    resident.descriptor.saturating_add(resident.block_source),
                    resident.column_payload,
                    idle_cycles,
                ));
            }
        }
        stats
    }

    /// Relative artifact-backed payload paths ranked by the existing volume-access epoch.
    /// `u64::MAX` means the volume was touched since the latest eviction pass;
    /// otherwise larger epochs are more recent. The page-cache warmup consumes
    /// this bounded metadata snapshot without reading payload blocks.
    pub fn page_cache_volume_priorities(&self) -> Vec<(PathBuf, u64)> {
        let manifest = self.manifest.read();
        let segments = self.segments.load_full();
        manifest
            .segments
            .iter()
            .filter_map(|meta| {
                let segment = segments.get(&meta.segment_id)?;
                Some((
                    meta.file_path.clone(),
                    segment
                        .volume
                        .last_access_epoch
                        .load(std::sync::atomic::Ordering::Relaxed),
                ))
            })
            .collect()
    }

    /// Current resident payload cache bytes for this table's segments.
    ///
    /// This excludes mandatory metadata (row ids, zone maps, descriptors) and
    /// only counts evictable materialized column payloads.
    pub fn volume_cache_bytes(&self) -> usize {
        self.segments
            .load()
            .values()
            .map(|cs| cs.volume.cache_memory_size())
            .sum()
    }

    /// Evict resident volume payload to fit a byte budget.
    ///
    /// Artifact-backed materialized volumes transition directly to
    /// metadata-only state while retaining their immutable DATA source.
    ///
    /// Eviction is budget-driven: idle epochs only rank candidates. A volume
    /// touched since the previous eviction pass is given one grace pass to
    /// reduce query thrash; after that, the largest/oldest candidates are
    /// demoted until this table reaches `max_cache_bytes` or no safe target
    /// remains.
    ///
    /// Returns the estimated number of payload bytes removed.
    pub fn evict_volumes_to_budget(&self, current_epoch: u64, max_cache_bytes: usize) -> usize {
        // Publish current epoch so scanners can stamp volumes correctly and
        // PRAGMA VOLUME_STATS can report idle cycles.
        self.current_eviction_epoch
            .store(current_epoch, std::sync::atomic::Ordering::Relaxed);

        #[derive(Debug)]
        struct Candidate {
            seg_id: u64,
            cache_bytes: usize,
            post_step_cache_bytes: usize,
            idle_cycles: u64,
        }

        // Identify candidates from an ArcSwap snapshot. Reset accessed
        // sentinel epochs so their idle counter starts from this cycle, but do
        // not evict them in the same pass.
        let (total_cache_bytes, mut candidates): (usize, Vec<Candidate>) = {
            let segs = self.segments.load_full();
            let mut total = 0usize;
            let mut candidates = Vec::new();
            for (&seg_id, cs) in segs.iter() {
                let cache_bytes = cs.volume.cache_memory_size();
                total = total.saturating_add(cache_bytes);
                if cache_bytes == 0 {
                    continue;
                }

                if !cs.volume.columns.is_eager() || cs.volume.artifact_source().is_none() {
                    continue;
                }

                let post_step_cache_bytes = cs.volume.cache_memory_size_after_one_eviction_step();
                if post_step_cache_bytes >= cache_bytes {
                    continue;
                }

                let vol_epoch = cs
                    .volume
                    .last_access_epoch
                    .load(std::sync::atomic::Ordering::Relaxed);
                if vol_epoch == u64::MAX {
                    cs.volume
                        .last_access_epoch
                        .store(current_epoch, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }

                candidates.push(Candidate {
                    seg_id,
                    cache_bytes,
                    post_step_cache_bytes,
                    idle_cycles: current_epoch.saturating_sub(vol_epoch),
                });
            }
            (total, candidates)
        };

        if total_cache_bytes <= max_cache_bytes || candidates.is_empty() {
            return 0;
        }

        candidates.sort_by(|a, b| {
            b.idle_cycles
                .cmp(&a.idle_cycles)
                .then_with(|| b.cache_bytes.cmp(&a.cache_bytes))
                .then_with(|| a.seg_id.cmp(&b.seg_id))
        });

        let mut remaining_cache_bytes = total_cache_bytes;
        let mut targets = Vec::new();
        for candidate in candidates {
            if remaining_cache_bytes <= max_cache_bytes {
                break;
            }
            remaining_cache_bytes = remaining_cache_bytes.saturating_sub(
                candidate
                    .cache_bytes
                    .saturating_sub(candidate.post_step_cache_bytes),
            );
            targets.push(candidate.seg_id);
        }

        if targets.is_empty() {
            return 0;
        }

        // Apply transitions under the writer-only CoW mutex. Readers use
        // ArcSwap loads and never take this lock.
        let _segments_guard = self.segments_update.lock();
        let mut new_map = (*self.segments.load_full()).clone();
        let mut freed_bytes = 0usize;
        for &seg_id in &targets {
            if let Some(cs) = new_map.get_mut(&seg_id) {
                let before = cs.volume.cache_memory_size();
                let cold = cs.volume.to_cold();
                let after = cold.cache_memory_size();
                freed_bytes = freed_bytes.saturating_add(before.saturating_sub(after));
                cs.volume = Arc::new(cold);
            }
        }
        self.segments.store(Arc::new(new_map));
        if freed_bytes > 0 {
            instrumentation::record_ram_accelerator_eviction(freed_bytes as u64);
        }
        freed_bytes
    }

    /// Count segments below the target row count (sub-target volumes that need merging).
    pub fn sub_target_segment_count(&self, target_rows: usize) -> usize {
        let manifest = self.manifest.read();
        manifest
            .segments
            .iter()
            .filter(|s| s.row_count < target_rows)
            .count()
    }

    /// Check if a segment with the given ID is already registered (loaded in memory).
    pub fn has_segment(&self, segment_id: u64) -> bool {
        self.segments.load().contains_key(&segment_id)
    }

    /// Check if a segment exists in the manifest (source of truth for what should be loaded).
    /// This metadata-only check does not load volume data.
    pub fn manifest_has_segment(&self, segment_id: u64) -> bool {
        self.manifest
            .read()
            .segments
            .iter()
            .any(|s| s.segment_id == segment_id)
    }

    /// Register a new segment after seal, compaction, or load.
    pub fn register_segment(
        &self,
        segment_id: u64,
        volume: Arc<FrozenVolume>,
        meta: SegmentMeta,
        schema: Option<&radixdb_core::Schema>,
    ) -> Result<()> {
        self.register_segments_atomic(
            vec![SegmentRegistration::new(segment_id, volume, meta)],
            schema,
            None,
        )
    }

    /// Validate and publish a complete seal batch as one manifest/map state.
    /// No member becomes visible unless every member is coherent and durable.
    pub fn register_segments_atomic(
        &self,
        mut registrations: Vec<SegmentRegistration>,
        schema: Option<&radixdb_core::Schema>,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        if registrations.is_empty() {
            return Ok(());
        }

        let mut batch_ids = FxHashSet::default();
        for registration in &mut registrations {
            self.normalize_new_segment_path(&mut registration.meta)?;
            self.validate_segment_before_register(registration, expected_schema_version)?;
            if !batch_ids.insert(registration.segment_id) {
                return Err(Error::internal(format!(
                    "refusing to register duplicate segment {} for {} in one batch",
                    registration.segment_id,
                    self.table_name()
                )));
            }
        }

        // Prepare the complete next manifest and segment map before publishing
        // either one. All fallible validation is complete at this point.
        let mut manifest = self.manifest.write();
        let _segments_guard = self.segments_update.lock();
        let mut next_manifest = manifest.clone();
        let mut next_map = (*self.segments.load_full()).clone();
        for registration in registrations {
            if next_manifest
                .segments
                .iter()
                .any(|meta| meta.segment_id == registration.segment_id)
                || next_map.contains_key(&registration.segment_id)
            {
                return Err(Error::internal(format!(
                    "refusing to register existing segment {} for {}",
                    registration.segment_id,
                    self.table_name()
                )));
            }

            let segment_id = registration.segment_id;
            let segment_schema_version = registration.meta.schema_version;
            let mapping = if let Some(schema) = schema {
                super::super::writer::compute_column_mapping_with_drops(
                    schema,
                    &registration.volume,
                    &next_manifest.dropped_columns,
                    segment_schema_version,
                    &next_manifest.column_renames,
                )
            } else {
                super::super::writer::ColumnMapping::identity(&registration.volume)
            };
            let cold =
                ColdSegment::new(registration.volume, mapping, segment_schema_version, None)?;
            if segment_id >= next_manifest.next_segment_id {
                next_manifest.next_segment_id = segment_id + 1;
            }
            next_manifest.add_segment(registration.meta);
            next_map.insert(segment_id, cold);
        }

        let segment_ids: Vec<u64> = next_manifest
            .segments
            .iter()
            .map(|meta| meta.segment_id)
            .collect();
        if selected_ranges_are_isolated(&batch_ids, &next_map, true) {
            // Existing bitmaps remain authoritative because the new physical
            // ranges cannot contain an old row ID. New segments have no
            // duplicates and are therefore wholly visible.
            for segment_id in &batch_ids {
                if let Some(segment) = next_map.get_mut(segment_id) {
                    segment.visible = None;
                }
            }
        } else {
            compute_visibility_bitmaps(
                &segment_ids,
                &mut next_map,
                &mut self.visibility_seen.lock(),
            );
        }
        *manifest = next_manifest;
        self.segments.store(Arc::new(next_map));
        self.mark_segment_topology_changed();
        self.has_segments_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.seal_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Normalize a newly produced durable path before it enters the manifest.
    /// Existing manifests are never repaired here: restart validates their
    /// stored relative identity exactly.
    pub(super) fn normalize_new_segment_path(&self, meta: &mut SegmentMeta) -> Result<()> {
        use std::path::Component;

        let Some(volume_dir) = self.volume_dir.as_ref() else {
            return Ok(());
        };
        if meta.file_path.as_os_str().is_empty() {
            return Ok(());
        }

        let table_name = self.table_name();
        if meta.file_path.is_absolute() {
            let canonical_root = std::fs::canonicalize(volume_dir).map_err(|error| {
                Error::internal(format!(
                    "failed to canonicalize volume directory {:?}: {}",
                    volume_dir, error
                ))
            })?;
            let canonical_table = std::fs::canonicalize(volume_dir.join(table_name.as_str()))
                .map_err(|error| {
                    Error::internal(format!(
                        "failed to canonicalize table directory for {}: {}",
                        table_name, error
                    ))
                })?;
            let canonical_path = std::fs::canonicalize(&meta.file_path).map_err(|error| {
                Error::internal(format!(
                    "segment {} volume file {:?} is missing or inaccessible: {}",
                    meta.segment_id, meta.file_path, error
                ))
            })?;
            if !canonical_path.starts_with(&canonical_table) {
                return Err(Error::internal(format!(
                    "refusing segment {} path {:?}: it is outside table directory {:?}",
                    meta.segment_id, meta.file_path, canonical_table
                )));
            }
            meta.file_path = canonical_path
                .strip_prefix(&canonical_root)
                .map_err(|_| Error::internal("canonical segment path escaped volume root"))?
                .to_path_buf();
            return Ok(());
        }

        if meta
            .file_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(Error::internal(format!(
                "segment {} path {:?} is not canonical",
                meta.segment_id, meta.file_path
            )));
        }
        if meta.file_path.components().count() == 1 {
            meta.file_path = PathBuf::from(table_name.as_str()).join(&meta.file_path);
        }
        Ok(())
    }

    pub(super) fn validate_segment_before_register(
        &self,
        registration: &SegmentRegistration,
        expected_schema_version: Option<u64>,
    ) -> Result<()> {
        let segment_id = registration.segment_id;
        let volume = registration.volume.as_ref();
        let meta = &registration.meta;
        let table_name = self.table_name();
        if segment_id == 0 || meta.segment_id != segment_id {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: metadata segment id is {}",
                segment_id, table_name, meta.segment_id
            )));
        }
        if expected_schema_version.is_some_and(|expected| meta.schema_version != expected) {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: schema version {} does not match captured generation {}",
                segment_id,
                table_name,
                meta.schema_version,
                expected_schema_version.unwrap()
            )));
        }
        if meta.row_count != volume.meta.row_count || meta.row_count != volume.meta.row_ids.len() {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: row count metadata {} disagrees with volume {}",
                segment_id, table_name, meta.row_count, volume.meta.row_count
            )));
        }
        let expected_bounds = volume
            .meta
            .row_ids
            .first()
            .zip(volume.meta.row_ids.last())
            .unwrap_or((0, 0));
        if (meta.min_row_id, meta.max_row_id) != expected_bounds {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: row-id bounds {:?} disagree with volume {:?}",
                segment_id,
                table_name,
                (meta.min_row_id, meta.max_row_id),
                expected_bounds
            )));
        }
        if volume.meta.column_names.len() != volume.meta.column_types.len()
            || volume.meta.column_names.len() != volume.columns.len()
        {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: volume column metadata is inconsistent",
                segment_id, table_name
            )));
        }
        if let Some(source) = volume.artifact_source() {
            let reference = source.layout().reference();
            if reference.relative_path() != meta.file_path {
                return Err(Error::internal(format!(
                    "refusing to register segment {} for {}: DATA path differs from runtime metadata",
                    segment_id, table_name
                )));
            }
            if source.row_count()? != meta.row_count {
                return Err(Error::internal(format!(
                    "refusing to register segment {} for {}: DATA row count differs from runtime metadata",
                    segment_id, table_name
                )));
            }
        } else if !volume.columns.is_eager() {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: metadata-only volume has no DATA source",
                segment_id, table_name
            )));
        }
        if meta.file_path.as_os_str().is_empty() {
            return Err(Error::internal(format!(
                "refusing to register segment {} for {}: empty volume path",
                meta.segment_id, table_name
            )));
        }

        Ok(())
    }

    /// Rename this process-local topology. Durable catalog/table identity and
    /// artifact reachability are published by the database generation owner;
    /// immutable artifact paths are independent of SQL table names.
    pub fn rename_table_storage(&self, new_name: &str) -> Result<()> {
        let _seal_guard = self.seal_fence.write();
        let old_name = self.table_name();
        if old_name.as_str() == new_name {
            return Ok(());
        }
        self.manifest.write().table_name = SmartString::from(new_name);
        *self.table_name.write() = SmartString::from(new_name);
        Ok(())
    }
}
