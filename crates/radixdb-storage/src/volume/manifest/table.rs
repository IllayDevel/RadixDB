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

//! Process-local table topology used by the MVCC runtime.
//!
//! Durable table membership is owned exclusively by the CONTROL-selected
//! artifact-generation manifest. This model deliberately has no filesystem
//! codec and cannot become a second persistence authority.

use super::*;

/// Physical placement level of one immutable runtime segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum SegmentLevel {
    /// Segment recovered from an artifact manifest before runtime scheduling.
    #[default]
    Unleveled = 0,
    /// Fresh immutable seal output.
    L0 = 1,
    /// First bounded stable level.
    L1 = 2,
}

/// Metadata for a single immutable runtime segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Process-local monotonic segment identifier.
    pub segment_id: u64,
    /// Canonical DATA artifact path relative to the database root.
    pub file_path: PathBuf,
    /// Number of rows in this segment.
    pub row_count: usize,
    /// Minimum row_id in this segment.
    pub min_row_id: i64,
    /// Maximum row_id in this segment.
    pub max_row_id: i64,
    /// Commit sequence at which this segment was sealed.
    pub seal_seq: u64,
    /// Runtime schema epoch when this segment was registered.
    pub schema_version: u64,
    /// Physical publication level.
    pub level: SegmentLevel,
    /// Commit/topology epoch at which this immutable segment was created.
    pub creation_epoch: u64,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Default for SegmentMeta {
    fn default() -> Self {
        Self {
            segment_id: 0,
            file_path: PathBuf::new(),
            row_count: 0,
            min_row_id: 0,
            max_row_id: 0,
            seal_seq: 0,
            schema_version: 0,
            level: SegmentLevel::Unleveled,
            creation_epoch: 0,
        }
    }
}

/// Immutable ownership proof captured before a compaction job starts.
///
/// The token deliberately names only the selected inputs. Unrelated appends
/// are compatible; replacement, relocation or mutation of any selected input
/// changes its effective metadata/fingerprint and invalidates publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionToken {
    pub(super) table_name: SmartString,
    pub(super) schema_epoch: u64,
    pub(super) inputs: Vec<SegmentMeta>,
    pub(super) tombstone_boundary: Option<u64>,
    pub(super) target_level: SegmentLevel,
}

impl CompactionToken {
    pub fn input_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.inputs.iter().map(|input| input.segment_id)
    }

    pub fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    pub fn tombstone_boundary(&self) -> Option<u64> {
        self.tombstone_boundary
    }

    pub fn target_level(&self) -> SegmentLevel {
        self.target_level
    }
}

#[derive(Clone, Copy)]
pub(super) enum ReplacementExpectation<'a> {
    Unchecked,
    #[cfg(test)]
    SegmentGeneration(u64),
    Compaction(&'a CompactionToken),
}

/// One immutable segment prepared for a single atomic topology publication.
pub struct SegmentRegistration {
    pub segment_id: u64,
    pub volume: Arc<FrozenVolume>,
    pub meta: SegmentMeta,
}

impl SegmentRegistration {
    pub fn new(segment_id: u64, volume: Arc<FrozenVolume>, meta: SegmentMeta) -> Self {
        Self {
            segment_id,
            volume,
            meta,
        }
    }
}

/// Process-local segment topology for one table.
///
/// The selected artifact generation is the durable source of truth. This
/// structure owns only runtime ordering, visibility state and schema-evolution
/// mappings needed by already-open segments.
#[derive(Debug, Clone)]
pub struct TableManifest {
    /// Table name.
    pub table_name: SmartString,
    /// Live segments ordered by process-local segment ID, oldest first.
    pub segments: Vec<SegmentMeta>,
    /// Next process-local segment ID to assign.
    pub next_segment_id: u64,
    /// Tombstone entries: (row_id, commit_seq) pairs for cold rows that have
    /// been deleted or superseded by hot buffer versions.
    pub tombstones: Vec<(i64, u64)>,
    /// Runtime column rename history for already-open physical schemas.
    pub column_renames: Vec<(SmartString, SmartString)>,
    /// Runtime dropped-column epochs for already-open physical schemas.
    pub dropped_columns: Vec<(SmartString, u64)>,
}

impl TableManifest {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: SmartString::from(table_name),
            segments: Vec::new(),
            next_segment_id: 1,
            tombstones: Vec::new(),
            column_renames: Vec::new(),
            dropped_columns: Vec::new(),
        }
    }

    /// Allocate a new process-local segment ID.
    pub fn allocate_segment_id(&mut self) -> u64 {
        let id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        id
    }

    pub fn add_segment(&mut self, meta: SegmentMeta) {
        self.next_segment_id = self.next_segment_id.max(meta.segment_id.saturating_add(1));
        self.segments.push(meta);
    }

    pub fn remove_segments(&mut self, ids: &[u64]) {
        let id_set: FxHashSet<u64> = ids.iter().copied().collect();
        self.segments
            .retain(|segment| !id_set.contains(&segment.segment_id));
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn segments(&self) -> &[SegmentMeta] {
        &self.segments
    }

    pub fn next_segment_id(&self) -> u64 {
        self.next_segment_id
    }

    pub fn tombstones(&self) -> &[(i64, u64)] {
        &self.tombstones
    }

    pub fn column_renames(&self) -> &[(SmartString, SmartString)] {
        &self.column_renames
    }

    pub fn dropped_columns(&self) -> &[(SmartString, u64)] {
        &self.dropped_columns
    }
}
