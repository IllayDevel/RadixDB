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

use super::*;

fn persisted_segment_fixture(
    artifact_dir: &Path,
    _table_name: &str,
    schema: &radixdb_core::Schema,
    segment_id: u64,
    row_ids: &[i64],
    level: SegmentLevel,
) -> (Arc<FrozenVolume>, SegmentMeta) {
    use radixdb_core::{Row, Value};

    let rows = row_ids
        .iter()
        .map(|&row_id| (row_id, Row::from_values(vec![Value::Integer(row_id)])))
        .collect::<Vec<_>>();
    let fixture = crate::volume::test_artifact::build_artifact_volume(
        artifact_dir,
        schema,
        segment_id,
        &rows,
    );
    let meta = SegmentMeta {
        segment_id,
        file_path: fixture.relative_path,
        row_count: row_ids.len(),
        min_row_id: row_ids.first().copied().unwrap_or(0),
        max_row_id: row_ids.last().copied().unwrap_or(0),
        seal_seq: segment_id,
        schema_version: 0,
        level,
        creation_epoch: segment_id,
    };
    (fixture.volume, meta)
}

#[test]
fn test_manifest_new() {
    let m = TableManifest::new("test_table");
    assert_eq!(m.table_name.as_str(), "test_table");
    assert!(m.segments.is_empty());
    assert_eq!(m.next_segment_id, 1);
    assert!(m.tombstones.is_empty());
}

#[test]
fn test_manifest_allocate_id() {
    let mut m = TableManifest::new("t");
    assert_eq!(m.allocate_segment_id(), 1);
    assert_eq!(m.allocate_segment_id(), 2);
    assert_eq!(m.allocate_segment_id(), 3);
    assert_eq!(m.next_segment_id, 4);
}

#[test]
fn v2_r3_visibility_bitmap_shape_is_validated_and_bounds_safe() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    for row_id in 0..65 {
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
    }
    let volume = Arc::new(builder.finish());
    let mapping = ColumnMapping::identity(&volume);

    assert!(ColdSegment::new(
        Arc::clone(&volume),
        mapping.clone(),
        0,
        Some(Arc::new(vec![u64::MAX])),
    )
    .is_err());
    assert!(ColdSegment::new(
        Arc::clone(&volume),
        mapping.clone(),
        0,
        Some(Arc::new(vec![u64::MAX, 0b11])),
    )
    .is_err());

    let segment =
        ColdSegment::new(volume, mapping, 0, Some(Arc::new(vec![u64::MAX, 0b1]))).unwrap();
    assert!(segment.is_visible(64));
    assert!(!segment.is_visible(65));
    assert!(!segment.is_visible(usize::MAX));
}

#[test]
fn v2_r3_authoritative_cold_read_error_never_falls_back_to_older_row() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let dir = tempfile::tempdir().unwrap();
    let artifact_dir = dir.path().join("artifacts");
    let manager = SegmentManager::new("test", Some(artifact_dir.clone()));

    let mut newest_path = None;
    for (segment_id, name) in [(1, "older"), (2, "newest")] {
        let rows = vec![(
            1,
            Row::from_values(vec![Value::Integer(1), Value::text(name)]),
        )];
        let fixture = crate::volume::test_artifact::build_artifact_volume(
            &artifact_dir,
            &schema,
            segment_id,
            &rows,
        );
        manager
            .register_segment(
                segment_id,
                fixture.volume,
                SegmentMeta {
                    segment_id,
                    file_path: fixture.relative_path,
                    row_count: 1,
                    min_row_id: 1,
                    max_row_id: 1,
                    seal_seq: segment_id,
                    schema_version: 0,
                    ..Default::default()
                },
                Some(&schema),
            )
            .unwrap();
        if segment_id == 2 {
            newest_path = Some(fixture.absolute_path);
        }
    }

    let newest_path = newest_path.unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&newest_path)
        .unwrap()
        .set_len(64)
        .unwrap();

    assert!(manager.get_cold_row(1).is_err());
    assert!(manager.get_cold_row_normalized(1, &schema).is_err());
    assert!(manager.get_authoritative_value(1, 1).is_err());
}

#[test]
fn test_manifest_add_remove_segment() {
    let mut m = TableManifest::new("t");
    m.add_segment(SegmentMeta {
        segment_id: 1,
        file_path: PathBuf::from("vol_001.data"),
        row_count: 1000,
        min_row_id: 1,
        max_row_id: 1000,
        seal_seq: 0,
        schema_version: 0,
        ..Default::default()
    });
    m.add_segment(SegmentMeta {
        segment_id: 2,
        file_path: PathBuf::from("vol_002.data"),
        row_count: 500,
        min_row_id: 1001,
        max_row_id: 1500,
        seal_seq: 0,
        schema_version: 0,
        ..Default::default()
    });
    assert_eq!(m.segments.len(), 2);

    m.remove_segments(&[1]);
    assert_eq!(m.segments.len(), 1);
    assert_eq!(m.segments[0].segment_id, 2);
}

#[test]
fn r3_l03_batch_a_rejects_incoherent_segment_identity_before_publish() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    builder.add_row(11, &Row::from_values(vec![Value::Integer(11)]));
    let volume = Arc::new(builder.finish());
    let mgr = SegmentManager::new("test", None);

    let error = mgr
        .register_segment(
            7,
            volume,
            SegmentMeta {
                segment_id: 8,
                file_path: PathBuf::from("test/vol_0000000000000008.data"),
                row_count: 2,
                min_row_id: 10,
                max_row_id: 12,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
            None,
        )
        .expect_err("incoherent segment metadata must fail before publication");

    assert!(error.to_string().contains("segment"));
    assert!(mgr.segments_raw().is_empty());
    assert!(!mgr.row_exists(11));
}

#[test]
fn r3_l03_batch_a_validates_complete_seal_batch_and_schema_generation() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_registration = |segment_id, row_id, schema_version| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        SegmentRegistration::new(
            segment_id,
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("test/vol_{segment_id:016x}.data")),
                row_count: 1,
                min_row_id: row_id,
                max_row_id: row_id,
                seal_seq: 0,
                schema_version,
                ..Default::default()
            },
        )
    };
    let mgr = SegmentManager::new("test", None);
    let error = mgr
        .register_segments_atomic(
            vec![make_registration(1, 1, 5), make_registration(2, 2, 6)],
            None,
            Some(5),
        )
        .expect_err("one stale batch member must reject the complete publication");
    assert!(error.to_string().contains("schema version"));
    assert_eq!(mgr.segment_count(), 0);
    assert!(!mgr.row_exists(1));
    assert!(!mgr.row_exists(2));

    mgr.register_segments_atomic(
        vec![make_registration(1, 1, 5), make_registration(2, 2, 5)],
        None,
        Some(5),
    )
    .unwrap();
    assert_eq!(mgr.segment_count(), 2);
    assert!(mgr.row_exists(1));
    assert!(mgr.row_exists(2));
}

#[test]
fn r8_l01_batch_d_seal_batch_publishes_topology_once() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_registration = |segment_id, row_id| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        SegmentRegistration::new(
            segment_id,
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("test/vol_{segment_id:016x}.data")),
                row_count: 1,
                min_row_id: row_id,
                max_row_id: row_id,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
        )
    };
    let mgr = SegmentManager::new("test", None);
    let topology_before = mgr.topology_generation();
    let segment_before = mgr.segment_generation();
    let seal_before = mgr.seal_generation();

    mgr.register_segments_atomic(
        vec![
            make_registration(1, 10),
            make_registration(2, 20),
            make_registration(3, 30),
        ],
        None,
        Some(0),
    )
    .unwrap();

    assert_eq!(mgr.segment_count(), 3);
    assert_eq!(mgr.topology_generation(), topology_before + 1);
    assert_eq!(mgr.segment_generation(), segment_before + 1);
    assert_eq!(mgr.seal_generation(), seal_before + 1);
    assert!(mgr.row_exists(10));
    assert!(mgr.row_exists(20));
    assert!(mgr.row_exists(30));
}

#[test]
fn tombstone_publication_does_not_invalidate_segment_snapshot() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_volume = |row_id| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        Arc::new(builder.finish())
    };
    let make_meta = |segment_id, row_id| SegmentMeta {
        segment_id,
        file_path: PathBuf::from(format!("test/vol_{segment_id:016x}.data")),
        row_count: 1,
        min_row_id: row_id,
        max_row_id: row_id,
        seal_seq: 0,
        schema_version: 0,
        ..Default::default()
    };
    let mgr = SegmentManager::new("test", None);
    mgr.register_segments_atomic(
        vec![SegmentRegistration::new(
            1,
            make_volume(42),
            make_meta(1, 42),
        )],
        None,
        Some(0),
    )
    .unwrap();
    let topology_before = mgr.topology_generation();
    let segment_before = mgr.segment_generation();

    mgr.add_tombstones(&[42], 7);

    assert_eq!(mgr.topology_generation(), topology_before + 1);
    assert_eq!(mgr.segment_generation(), segment_before);
    mgr.replace_segments_atomic_multi_checked(
        vec![(2, make_volume(42), make_meta(2, 42))],
        &[1],
        Some(segment_before),
        Some(0),
    )
    .unwrap();
    assert_eq!(mgr.segment_count(), 1);
    assert!(mgr.tombstone_set_arc().contains_key(&42));
}

#[test]
fn compaction_token_accepts_unrelated_append_and_preserves_it() {
    use radixdb_core::{DataType, SchemaBuilder};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let dir = tempfile::tempdir().unwrap();
    let artifact_dir = dir.path().join("artifacts");
    let mgr = SegmentManager::new("test", Some(artifact_dir.clone()));
    let (volume_a, meta_a) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 1, &[1], SegmentLevel::L0);
    let (volume_b, meta_b) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 2, &[2], SegmentLevel::L0);
    mgr.register_segments_atomic(
        vec![
            SegmentRegistration::new(1, volume_a, meta_a),
            SegmentRegistration::new(2, volume_b, meta_b),
        ],
        None,
        Some(0),
    )
    .unwrap();

    let published = mgr.segments_raw();
    let manifest = mgr.manifest();
    let token = mgr
        .capture_compaction_token_from_snapshot(
            &manifest,
            &published,
            &[1, 2],
            0,
            Some(44),
            SegmentLevel::L1,
        )
        .unwrap();
    drop(manifest);
    drop(published);
    let generation_before_append = mgr.segment_generation();

    let (volume_c, meta_c) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 3, &[3], SegmentLevel::L0);
    mgr.register_segments_atomic(
        vec![SegmentRegistration::new(3, volume_c, meta_c)],
        None,
        Some(0),
    )
    .unwrap();
    mgr.add_tombstones(&[99], 45);
    assert!(mgr.segment_generation() > generation_before_append);
    mgr.validate_compaction_token_live(&token, 0).unwrap();

    let published = mgr.segments_raw();
    let manifest = mgr.manifest();
    let noncontiguous = mgr
        .capture_compaction_token_from_snapshot(
            &manifest,
            &published,
            &[1, 3],
            0,
            Some(44),
            SegmentLevel::L1,
        )
        .unwrap_err();
    assert!(noncontiguous
        .to_string()
        .contains("contiguous manifest range"));
    drop(manifest);
    drop(published);

    let (volume_ab, meta_ab) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 4, &[1, 2], SegmentLevel::L1);
    mgr.replace_segments_atomic_multi_compaction_checked(vec![(4, volume_ab, meta_ab)], &token, 0)
        .unwrap();

    let manifest = mgr.manifest();
    assert_eq!(
        manifest
            .segments
            .iter()
            .map(|meta| (meta.segment_id, meta.level))
            .collect::<Vec<_>>(),
        vec![(4, SegmentLevel::L1), (3, SegmentLevel::L0)]
    );
    drop(manifest);
    assert!(mgr.row_exists(1));
    assert!(mgr.row_exists(2));
    assert!(mgr.row_exists(3));
    assert!(mgr.tombstone_set_arc().contains_key(&99));
    assert_eq!(token.tombstone_boundary(), Some(44));
}

#[test]
fn compaction_token_rejects_schema_or_input_replacement() {
    use radixdb_core::{DataType, SchemaBuilder};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let dir = tempfile::tempdir().unwrap();
    let artifact_dir = dir.path().join("artifacts");
    let mgr = SegmentManager::new("test", Some(artifact_dir.clone()));
    let (volume_a, meta_a) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 1, &[1], SegmentLevel::L0);
    let (volume_b, meta_b) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 2, &[2], SegmentLevel::L0);
    mgr.register_segments_atomic(
        vec![
            SegmentRegistration::new(1, volume_a, meta_a),
            SegmentRegistration::new(2, volume_b, meta_b),
        ],
        None,
        Some(0),
    )
    .unwrap();
    let published = mgr.segments_raw();
    let manifest = mgr.manifest();
    let token = mgr
        .capture_compaction_token_from_snapshot(
            &manifest,
            &published,
            &[1, 2],
            0,
            None,
            SegmentLevel::L1,
        )
        .unwrap();
    drop(manifest);
    drop(published);

    let live_schema_error = mgr.validate_compaction_token_live(&token, 1).unwrap_err();
    assert!(live_schema_error
        .to_string()
        .contains("schema epoch changed"));
    let schema_error = mgr
        .replace_segments_atomic_remove_only_compaction_checked(&token, 1)
        .unwrap_err();
    assert!(schema_error.to_string().contains("schema epoch changed"));
    assert_eq!(mgr.segment_count(), 2);

    let (wrong_level, wrong_level_meta) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 4, &[1, 2], SegmentLevel::L0);
    let level_error = mgr
        .replace_segments_atomic_multi_compaction_checked(
            vec![(4, wrong_level, wrong_level_meta)],
            &token,
            0,
        )
        .unwrap_err();
    assert!(level_error.to_string().contains("does not match target"));
    assert_eq!(mgr.segment_count(), 2);

    let (replacement, replacement_meta) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 3, &[1], SegmentLevel::L0);
    mgr.replace_segments_atomic(3, replacement, replacement_meta, &[1])
        .unwrap();
    let live_input_error = mgr.validate_compaction_token_live(&token, 0).unwrap_err();
    assert!(live_input_error.to_string().contains("no longer live"));
    let (staged, staged_meta) =
        persisted_segment_fixture(&artifact_dir, "test", &schema, 5, &[1, 2], SegmentLevel::L1);
    let conflict = mgr
        .replace_segments_atomic_multi_compaction_checked(vec![(5, staged, staged_meta)], &token, 0)
        .unwrap_err();
    assert!(conflict.to_string().contains("no longer live"));
    assert_eq!(
        mgr.manifest()
            .segments
            .iter()
            .map(|meta| meta.segment_id)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    assert!(!mgr.segments_raw().contains_key(&5));
}

#[test]
fn r3_l03_batch_a_stale_exact_count_cannot_overwrite_topology_invalidation() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_volume = |row_id| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        Arc::new(builder.finish())
    };
    let mgr = SegmentManager::new("test", None);
    mgr.register_segment(
        1,
        make_volume(1),
        SegmentMeta {
            segment_id: 1,
            file_path: PathBuf::from("test/vol_0000000000000001.data"),
            row_count: 1,
            min_row_id: 1,
            max_row_id: 1,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(mgr.deduped_row_count(), 1);

    let stale_count = mgr.compute_deduped_row_count();
    mgr.register_segment(
        2,
        make_volume(2),
        SegmentMeta {
            segment_id: 2,
            file_path: PathBuf::from("test/vol_0000000000000002.data"),
            row_count: 1,
            min_row_id: 2,
            max_row_id: 2,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    // Reproduce the losing interleaving: an exact-count reader computed
    // before the second publication and stores after its invalidation.
    mgr.cached_deduped_count
        .store(stale_count as u64, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(mgr.deduped_row_count(), 2);
}

#[test]
fn test_segment_manager_register_and_query() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();

    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    for i in 1..=10i64 {
        builder.add_row(i, &Row::from_values(vec![Value::Integer(i)]));
    }
    let volume = Arc::new(builder.finish());

    let mgr = SegmentManager::new("test", None);
    let meta = SegmentMeta {
        segment_id: 1,
        file_path: PathBuf::from("test.data"),
        row_count: 10,
        min_row_id: 1,
        max_row_id: 10,
        seal_seq: 0,
        schema_version: 0,
        ..Default::default()
    };
    mgr.register_segment(1, volume, meta, None).unwrap();

    assert_eq!(mgr.segment_count(), 1);
    assert_eq!(mgr.total_row_count(), 10);
    assert!(mgr.row_exists(5));
    assert!(!mgr.row_exists(11));
}

#[test]
fn pending_tombstone_journal_rolls_back_only_savepoint_suffix() {
    let mgr = SegmentManager::new("test", None);

    mgr.add_pending_tombstone(7, 1);
    let savepoint = crate::timestamp::get_fast_timestamp();
    mgr.add_pending_tombstones(7, &[2, 3]);
    mgr.rollback_pending_tombstones_to_timestamp(7, savepoint);

    assert!(mgr.is_pending_tombstone(7, 1));
    assert!(!mgr.is_pending_tombstone(7, 2));
    assert!(!mgr.is_pending_tombstone(7, 3));
    assert_eq!(mgr.pending_tombstone_count(7), 1);
}

#[test]
fn test_cold_snapshot_batch_membership_preserves_alignment_and_visibility() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    for id in 1..=4_i64 {
        builder.add_row(id, &Row::from_values(vec![Value::Integer(id)]));
    }

    let mgr = SegmentManager::new("test", None);
    mgr.register_segment(
        1,
        Arc::new(builder.finish()),
        SegmentMeta {
            segment_id: 1,
            file_path: PathBuf::from("test.data"),
            row_count: 4,
            min_row_id: 1,
            max_row_id: 4,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    mgr.add_pending_tombstone(7, 2);
    mgr.add_tombstones(&[4], 20);
    let snapshot = mgr.cold_snapshot();
    let ids = [1, 2, 2, 4, 11];

    let mut old_snapshot_matches = [false; 5];
    let old_snapshot_hits = mgr.merge_visible_row_ids_from_cold_snapshot(
        &snapshot,
        7,
        Some(10),
        &ids,
        &mut old_snapshot_matches,
    );
    assert_eq!(old_snapshot_matches, [true, false, false, true, false]);
    assert_eq!(old_snapshot_hits, 2);

    let mut other_txn_matches = [false; 5];
    let other_txn_hits = mgr.merge_visible_row_ids_from_cold_snapshot(
        &snapshot,
        8,
        Some(10),
        &ids,
        &mut other_txn_matches,
    );
    assert_eq!(other_txn_matches, [true, true, true, true, false]);
    assert_eq!(
        other_txn_hits, 4,
        "duplicate input positions count separately"
    );

    let mut current_matches = [false; 5];
    let current_hits = mgr.merge_visible_row_ids_from_cold_snapshot(
        &snapshot,
        7,
        None,
        &ids,
        &mut current_matches,
    );
    assert_eq!(current_matches, [true, false, false, false, false]);
    assert_eq!(current_hits, 1);

    let mut hot_wins = [false, true, true, false, false];
    let hot_wins_hits =
        mgr.merge_visible_row_ids_from_cold_snapshot(&snapshot, 7, None, &ids, &mut hot_wins);
    assert_eq!(hot_wins, [true, true, true, false, false]);
    assert_eq!(hot_wins_hits, 3);
}

#[test]
fn test_cold_snapshot_batch_membership_honors_newest_overlapping_segment() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mgr = SegmentManager::new("test", None);

    let mut old_builder = super::super::writer::VolumeBuilder::new(&schema);
    old_builder.add_row(1, &Row::from_values(vec![Value::Integer(1)]));
    old_builder.add_row(2, &Row::from_values(vec![Value::Integer(2)]));
    mgr.register_segment(
        1,
        Arc::new(old_builder.finish()),
        SegmentMeta {
            segment_id: 1,
            file_path: PathBuf::from("old.data"),
            row_count: 2,
            min_row_id: 1,
            max_row_id: 2,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    let mut new_builder = super::super::writer::VolumeBuilder::new(&schema);
    new_builder.add_row(2, &Row::from_values(vec![Value::Integer(2)]));
    new_builder.add_row(3, &Row::from_values(vec![Value::Integer(3)]));
    mgr.register_segment(
        2,
        Arc::new(new_builder.finish()),
        SegmentMeta {
            segment_id: 2,
            file_path: PathBuf::from("new.data"),
            row_count: 2,
            min_row_id: 2,
            max_row_id: 3,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    let snapshot = mgr.cold_snapshot();
    let old_segment = snapshot.segs.get(&1).unwrap();
    let new_segment = snapshot.segs.get(&2).unwrap();
    assert!(old_segment.is_visible(0));
    assert!(
        !old_segment.is_visible(1),
        "the stale row-id 2 copy must be masked by the newer segment"
    );
    assert!(new_segment.is_visible(0));

    let row_ids = [1, 2, 2, 3, 9];
    let mut matches = [false; 5];
    let hits =
        mgr.merge_visible_row_ids_from_cold_snapshot(&snapshot, 42, None, &row_ids, &mut matches);
    assert_eq!(matches, [true, true, true, true, false]);
    assert_eq!(hits, 4, "duplicate positions retain SQL multiplicity");

    let mut append_builder = super::super::writer::VolumeBuilder::new(&schema);
    append_builder.add_row(100, &Row::from_values(vec![Value::Integer(100)]));
    mgr.register_segment(
        3,
        Arc::new(append_builder.finish()),
        SegmentMeta {
            segment_id: 3,
            file_path: PathBuf::from("append.data"),
            row_count: 1,
            min_row_id: 100,
            max_row_id: 100,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let after_append = mgr.cold_snapshot();
    assert!(
        !after_append.segs.get(&1).unwrap().is_visible(1),
        "a disjoint append must retain the older overlap mask"
    );
    assert!(after_append.segs.get(&3).unwrap().is_visible(0));
}

#[test]
fn disjoint_append_publication_does_not_scan_historical_row_ids() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_registration = |segment_id: u64, first: i64, last: i64| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        for row_id in first..=last {
            builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        }
        SegmentRegistration::new(
            segment_id,
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("append-{segment_id}.data")),
                row_count: usize::try_from(last - first + 1).unwrap(),
                min_row_id: first,
                max_row_id: last,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
        )
    };
    let mgr = SegmentManager::new("test", None);
    mgr.register_segments_atomic(vec![make_registration(1, 1, 4_096)], None, Some(0))
        .unwrap();
    assert_eq!(mgr.visibility_seen.lock().capacity(), 0);

    mgr.register_segments_atomic(vec![make_registration(2, 4_097, 8_192)], None, Some(0))
        .unwrap();

    assert_eq!(
        mgr.visibility_seen.lock().capacity(),
        0,
        "disjoint append publication must not allocate a historical row-id set"
    );
    let snapshot = mgr.cold_snapshot();
    assert!(snapshot
        .segs
        .values()
        .all(|segment| segment.visible.is_none()));
    assert!(mgr.row_exists(1));
    assert!(mgr.row_exists(8_192));
}

#[test]
fn disjoint_compaction_replacement_does_not_scan_untouched_row_ids() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_segment = |segment_id: u64, first: i64, last: i64| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        for row_id in first..=last {
            builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        }
        (
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("compact-{segment_id}.data")),
                row_count: usize::try_from(last - first + 1).unwrap(),
                min_row_id: first,
                max_row_id: last,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
        )
    };
    let mgr = SegmentManager::new("test", None);
    for (segment_id, first, last) in [(1, 1, 4_096), (2, 4_097, 8_192), (3, 8_193, 12_288)] {
        let (volume, meta) = make_segment(segment_id, first, last);
        mgr.register_segment(segment_id, volume, meta, None)
            .unwrap();
    }
    assert_eq!(mgr.visibility_seen.lock().capacity(), 0);

    let (replacement, replacement_meta) = make_segment(4, 1, 8_192);
    mgr.replace_segments_atomic(4, replacement, replacement_meta, &[1, 2])
        .unwrap();

    assert_eq!(
        mgr.visibility_seen.lock().capacity(),
        0,
        "isolated compaction replacement must not scan untouched cold rows"
    );
    assert_eq!(mgr.segment_count(), 2);
    assert!(mgr.row_exists(1));
    assert!(mgr.row_exists(8_192));
    assert!(mgr.row_exists(12_288));
}

#[test]
fn retained_cold_snapshot_keeps_its_mapping_after_compaction_replacement() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let make_segment = |segment_id: u64, row_id: i64, name: &str| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(
            row_id,
            &Row::from_values(vec![Value::Integer(row_id), Value::text(name)]),
        );
        (
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("compact-{segment_id}.data")),
                row_count: 1,
                min_row_id: row_id,
                max_row_id: row_id,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
        )
    };

    let mgr = SegmentManager::new("test", None);
    let (original, original_meta) = make_segment(1, 1, "original");
    mgr.register_segment(1, original, original_meta, Some(&schema))
        .unwrap();
    let retained = mgr.get_volumes_newest_first_lazy();
    let retained_cold = retained[0].1.clone();

    let (replacement, replacement_meta) = make_segment(2, 1, "replacement");
    mgr.replace_segments_atomic(2, replacement, replacement_meta, &[1])
        .unwrap();

    assert!(
        mgr.get_volume_mapping(1, &schema).sources.is_empty(),
        "a second manager lookup cannot resolve a retired segment id"
    );
    let mapping = mgr.get_cold_segment_mapping(&retained_cold, &schema);
    assert!(mapping.is_identity);
    assert_eq!(
        retained_cold.volume.get_row_mapped(0, &mapping),
        Row::from_values(vec![Value::Integer(1), Value::text("original")]),
        "a reader that retained the old volume must retain its matching mapping"
    );
}

#[test]
fn test_metadata_only_segment_without_artifact_source_is_rejected() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    builder.add_row(1, &Row::from_values(vec![Value::Integer(1)]));
    let volume = Arc::new(builder.finish().to_cold());
    let mapping = super::super::writer::ColumnMapping::identity(&volume);

    let error = match ColdSegment::new(volume, mapping, 0, None) {
        Err(error) => error,
        Ok(_) => panic!("metadata-only segment without DATA source must be rejected"),
    };
    assert!(error.to_string().contains("without an artifact source"));
}
#[test]
fn test_segment_manager_tombstones() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();

    let mut builder = super::super::writer::VolumeBuilder::new(&schema);
    for i in 1..=10i64 {
        builder.add_row(i, &Row::from_values(vec![Value::Integer(i)]));
    }
    let volume = Arc::new(builder.finish());

    let mgr = SegmentManager::new("test", None);
    mgr.register_segment(
        1,
        volume,
        SegmentMeta {
            segment_id: 1,
            file_path: PathBuf::from("test.data"),
            row_count: 10,
            min_row_id: 1,
            max_row_id: 10,
            seal_seq: 0,
            schema_version: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    // Tombstone row_id=5 (commit_seq=1)
    mgr.add_tombstones(&[5], 1);
    assert!(!mgr.row_exists(5));
    assert!(mgr.row_exists(4));
    assert!(mgr.row_exists(6));
    assert_eq!(mgr.total_physical_row_count(), 10);
    assert_eq!(mgr.total_row_count(), 9);
    assert!(mgr.is_tombstoned(5));
    assert!(!mgr.is_tombstoned(4));

    // Clear tombstones
    mgr.clear_tombstones();
    assert!(mgr.row_exists(5));
    assert_eq!(mgr.total_physical_row_count(), 10);
    assert_eq!(mgr.total_row_count(), 10);
}

#[test]
fn test_segment_manager_volumes_newest_first() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .build();

    let mgr = SegmentManager::new("test", None);

    for seg_id in [1u64, 3, 2] {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(
            seg_id as i64,
            &Row::from_values(vec![Value::Integer(seg_id as i64)]),
        );
        let vol = Arc::new(builder.finish());
        mgr.register_segment(
            seg_id,
            vol,
            SegmentMeta {
                segment_id: seg_id,
                file_path: PathBuf::from(format!("vol_{}.data", seg_id)),
                row_count: 1,
                min_row_id: seg_id as i64,
                max_row_id: seg_id as i64,
                seal_seq: 0,
                schema_version: 0,
                level: match seg_id {
                    1 => SegmentLevel::Unleveled,
                    2 => SegmentLevel::L0,
                    _ => SegmentLevel::L1,
                },
                ..Default::default()
            },
            None,
        )
        .unwrap();
    }

    let newest_first = mgr.get_volumes_newest_first_lazy();
    assert_eq!(newest_first.len(), 3);
    // get_volumes_newest_first reverses manifest insertion order.
    // Segments were registered as [1, 3, 2], so reversed = [2, 3, 1].
    assert_eq!(newest_first[0].0, 2);
    assert_eq!(newest_first[1].0, 3);
    assert_eq!(newest_first[2].0, 1);

    let owner = mgr.runtime_owner_snapshot(10);
    assert_eq!(owner.segments, 3);
    assert_eq!(owner.unleveled_segments, 1);
    assert_eq!(owner.l0_segments, 1);
    assert_eq!(owner.l1_segments, 1);
    assert_eq!(mgr.l0_debt_snapshot().segments, 2);
    assert!(!owner.level_metadata_busy);
    assert!(!owner.truncated);

    let bounded = mgr.runtime_owner_snapshot(2);
    assert_eq!(
        bounded.unleveled_segments + bounded.l0_segments + bounded.l1_segments,
        2
    );
    assert!(bounded.truncated);
}

#[test]
fn test_segment_manager_clear() {
    let mgr = SegmentManager::new("test", None);
    mgr.manifest.write().add_segment(SegmentMeta {
        segment_id: 1,
        file_path: PathBuf::from("x.data"),
        row_count: 10,
        min_row_id: 1,
        max_row_id: 10,
        seal_seq: 0,
        schema_version: 0,
        ..Default::default()
    });
    mgr.add_tombstones(&[5], 1);

    assert_eq!(mgr.segment_count(), 1);
    mgr.clear();
    assert_eq!(mgr.segment_count(), 0);
    assert!(mgr.tombstone_set_arc().is_empty());
}

#[test]
fn tombstone_publication_generation_tracks_only_exact_set_changes() {
    let manager = SegmentManager::new("test", None);
    assert!(manager.tombstone_publication_snapshot().is_none());

    manager.add_tombstones(&[7], 11);
    let first = manager.tombstone_publication_snapshot().unwrap();
    assert_eq!(first.tombstones().get(&7), Some(&11));
    manager.confirm_tombstone_publication(first.generation());
    assert!(manager.tombstone_publication_snapshot().is_none());

    manager.add_tombstones(&[7], 11);
    assert!(manager.tombstone_publication_snapshot().is_none());

    manager.add_tombstones(&[7], 12);
    let replacement = manager.tombstone_publication_snapshot().unwrap();
    assert_eq!(replacement.tombstones().get(&7), Some(&12));
    manager.confirm_tombstone_publication(replacement.generation());

    manager.clear_tombstones();
    let empty = manager.tombstone_publication_snapshot().unwrap();
    assert!(empty.tombstones().is_empty());
    manager.confirm_tombstone_publication(empty.generation());
    assert!(manager.tombstone_publication_snapshot().is_none());
}

#[test]
fn r1_l04_batch_a_eviction_epochs_are_manager_local() {
    use radixdb_core::{DataType, Row, SchemaBuilder, Value};

    let schema = SchemaBuilder::new("epochs")
        .column("id", DataType::Integer, false, true)
        .build();
    let make_manager = |name: &str, segment_id: u64| {
        let mut builder = super::super::writer::VolumeBuilder::new(&schema);
        builder.add_row(
            segment_id as i64,
            &Row::from_values(vec![Value::Integer(segment_id as i64)]),
        );
        let mgr = SegmentManager::new(name, None);
        mgr.register_segment(
            segment_id,
            Arc::new(builder.finish()),
            SegmentMeta {
                segment_id,
                file_path: PathBuf::from(format!("vol_{segment_id}.data")),
                row_count: 1,
                min_row_id: segment_id as i64,
                max_row_id: segment_id as i64,
                seal_seq: 0,
                schema_version: 0,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        mgr
    };

    let manager_a = make_manager("a", 1);
    let manager_b = make_manager("b", 2);
    manager_a.evict_volumes_to_budget(100, usize::MAX);
    assert_eq!(
        manager_a
            .current_eviction_epoch
            .load(std::sync::atomic::Ordering::Relaxed),
        100
    );
    assert_eq!(
        manager_b
            .current_eviction_epoch
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "engine A eviction must not advance engine B's epoch domain"
    );
    manager_b.evict_volumes_to_budget(3, usize::MAX);
    assert_eq!(
        manager_b
            .current_eviction_epoch
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
}
