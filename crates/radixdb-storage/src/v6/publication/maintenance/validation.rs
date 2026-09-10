use std::collections::{HashMap, HashSet};

use radixdb_catalog::ObjectId;

use crate::v6::{
    ArtifactKind, ArtifactRef, FormatError, FormatResult, PhysicalGenerationSnapshot,
    SegmentDescriptor,
};

use super::MaintenanceKind;

pub(crate) fn validate_maintenance_transition(
    kind: MaintenanceKind,
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
) -> FormatResult<Vec<ArtifactRef>> {
    if target.control().catalog() != source.control().catalog()
        || target.control().wal_replay_floor() != source.control().wal_replay_floor()
    {
        return invalid("maintenance transition changes catalog or WAL authority");
    }
    validate_table_set(source, target)?;
    match kind {
        MaintenanceKind::Compaction => validate_compaction(source, target)?,
        MaintenanceKind::IndexRebuild => validate_index_rebuild(source, target)?,
    }
    Ok(retired_artifacts(source, target))
}

/// Prove that a rebased compaction changes exactly one table by replacing the
/// named immutable inputs with the named outputs. Independent tables and
/// segments appended after the original build must remain byte-identical in
/// the target graph.
pub(crate) fn validate_rebased_compaction_transition(
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
    table_id: ObjectId,
    removals: &[SegmentDescriptor],
    additions: &[SegmentDescriptor],
) -> FormatResult<Vec<ArtifactRef>> {
    let retired = validate_maintenance_transition(MaintenanceKind::Compaction, source, target)?;
    if removals.is_empty() {
        return invalid("rebased compaction has no selected inputs");
    }
    if target.database_manifest().transaction_high_water()
        < source.database_manifest().transaction_high_water()
    {
        return invalid("rebased compaction regresses transaction high water");
    }

    let source_table = source
        .table_manifest(table_id)
        .ok_or_else(|| invalid_error("rebased compaction table is absent from source"))?;
    let target_table = target
        .table_manifest(table_id)
        .ok_or_else(|| invalid_error("rebased compaction table is absent from target"))?;
    if source_table.row_id_high_water() != target_table.row_id_high_water() {
        return invalid("rebased compaction changes table row-ID high water");
    }
    if target_table.catalog_generation() != source.database_manifest().catalog().generation() {
        return invalid("rebased compaction table does not bind the current catalog generation");
    }

    for (source_other, target_other) in source
        .table_manifests()
        .iter()
        .zip(target.table_manifests())
    {
        if source_other.table_id() != table_id && source_other != target_other {
            return invalid("rebased compaction changes an independent table");
        }
    }

    let source_segments = source_table
        .segments()
        .iter()
        .copied()
        .map(|segment| (segment.id(), segment))
        .collect::<HashMap<_, _>>();
    let mut removal_ids = HashSet::with_capacity(removals.len());
    for removal in removals {
        if !removal_ids.insert(removal.id()) {
            return invalid("rebased compaction repeats a selected input");
        }
        if source_segments.get(&removal.id()) != Some(removal) {
            return invalid("rebased compaction selected input changed");
        }
    }

    let mut addition_ids = HashSet::with_capacity(additions.len());
    for addition in additions {
        if !addition_ids.insert(addition.id()) {
            return invalid("rebased compaction repeats an output segment");
        }
        if source_segments.contains_key(&addition.id()) {
            return invalid("rebased compaction reuses a live segment identity");
        }
    }

    let mut expected = source_table
        .segments()
        .iter()
        .copied()
        .filter(|segment| !removal_ids.contains(&segment.id()))
        .collect::<Vec<_>>();
    expected.extend_from_slice(additions);
    expected.sort_unstable_by_key(|segment| segment.id());
    if target_table.segments() != expected {
        return invalid("rebased compaction target differs from the exact segment patch");
    }
    let addition_count = u64::try_from(additions.len())
        .map_err(|_| invalid_error("rebased compaction output count exceeds u64"))?;
    let expected_sequence = source_table
        .next_segment_sequence()
        .checked_add(addition_count)
        .ok_or_else(|| invalid_error("rebased compaction segment sequence overflows"))?;
    if target_table.next_segment_sequence() != expected_sequence {
        return invalid("rebased compaction has the wrong segment allocator successor");
    }
    Ok(retired)
}

fn validate_table_set(
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
) -> FormatResult<()> {
    let source_tables = source
        .table_manifests()
        .iter()
        .map(|manifest| manifest.table_id())
        .collect::<Vec<_>>();
    let target_tables = target
        .table_manifests()
        .iter()
        .map(|manifest| manifest.table_id())
        .collect::<Vec<_>>();
    if source_tables != target_tables {
        return invalid("maintenance transition changes the table set");
    }
    for (source_table, target_table) in source
        .table_manifests()
        .iter()
        .zip(target.table_manifests())
    {
        if source_table.row_id_high_water() != target_table.row_id_high_water() {
            return invalid("maintenance transition changes row-ID high water");
        }
        if target_table.next_segment_sequence() < source_table.next_segment_sequence() {
            return invalid("maintenance transition regresses segment allocator");
        }
    }
    Ok(())
}

fn validate_compaction(
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
) -> FormatResult<()> {
    let source_data = artifacts_of_kind(source, ArtifactKind::Data);
    let target_data = artifacts_of_kind(target, ArtifactKind::Data);
    if source_data == target_data {
        return invalid("compaction does not replace any data artifact");
    }
    if !source_data.iter().any(|entry| !target_data.contains(entry)) {
        return invalid("compaction must retire a data artifact");
    }
    Ok(())
}

fn validate_index_rebuild(
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
) -> FormatResult<()> {
    let mut changed_index = false;
    for (source_table, target_table) in source
        .table_manifests()
        .iter()
        .zip(target.table_manifests())
    {
        if source_table.next_segment_sequence() != target_table.next_segment_sequence()
            || source_table.segments().len() != target_table.segments().len()
        {
            return invalid("index rebuild changes segment topology");
        }
        let target_segments = target_table
            .segments()
            .iter()
            .map(|segment| (segment.id(), *segment))
            .collect::<HashMap<_, _>>();
        for source_segment in source_table.segments() {
            let Some(target_segment) = target_segments.get(&source_segment.id()) else {
                return invalid("index rebuild changes segment identity");
            };
            if !same_data_descriptor(*source_segment, *target_segment) {
                return invalid("index rebuild changes authoritative segment data");
            }
            changed_index |= source_segment.index_artifact() != target_segment.index_artifact();
        }
    }
    if !changed_index {
        return invalid("index rebuild does not replace any accelerator artifact");
    }
    Ok(())
}

fn same_data_descriptor(source: SegmentDescriptor, target: SegmentDescriptor) -> bool {
    source.id() == target.id()
        && source.kind() == target.kind()
        && source.min_transaction_id() == target.min_transaction_id()
        && source.max_transaction_id() == target.max_transaction_id()
        && source.row_count() == target.row_count()
        && source.first_row_id() == target.first_row_id()
        && source.last_row_id() == target.last_row_id()
        && source.data_artifact() == target.data_artifact()
}

fn artifacts_of_kind(
    snapshot: &PhysicalGenerationSnapshot,
    kind: ArtifactKind,
) -> HashSet<ArtifactRef> {
    snapshot
        .artifact_references()
        .into_iter()
        .filter(|reference| reference.kind() == kind)
        .collect()
}

fn retired_artifacts(
    source: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
) -> Vec<ArtifactRef> {
    let target = target
        .artifact_references()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut retired = source
        .artifact_references()
        .into_iter()
        .filter(|reference| !target.contains(reference))
        .collect::<Vec<_>>();
    retired.sort_unstable_by_key(|reference| {
        let kind = match reference.kind() {
            ArtifactKind::Data => 0_u8,
            ArtifactKind::Index => 1_u8,
        };
        (kind, reference.id().into_bytes())
    });
    retired
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(invalid_error(detail))
}

const fn invalid_error(detail: &'static str) -> FormatError {
    FormatError::InvalidMaintenance { detail }
}
