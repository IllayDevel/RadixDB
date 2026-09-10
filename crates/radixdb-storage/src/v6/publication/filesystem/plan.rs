use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::v6::{
    encode_control_slot, validate_control_generation, ArtifactId, ArtifactKind, ArtifactRef,
    CompleteStagingSet, ControlRecord, ControlSlotIndex, FormatError, FormatResult, ManifestId,
    PhysicalGenerationSnapshot, StagingDiscoveryLimits, TableManifestRef, UnavailableIndex,
    CONTROL_RECORD_BYTES,
};

use super::source::{
    catalog_path, database_manifest_path, table_manifest_path, wal_path, GenerationFileSource,
};

pub(crate) struct GenerationPublicationPlan {
    expected_control: ControlRecord,
    target_control: ControlRecord,
    control_bytes: [u8; CONTROL_RECORD_BYTES],
    snapshot: PhysicalGenerationSnapshot,
    members: Vec<PublicationMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationMember {
    role: PublicationMemberRole,
    relative_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationMemberRole {
    Data,
    Index,
    TableManifest,
    CatalogPack,
    WalSuccessor,
    DatabaseManifest,
}

impl PublicationMember {
    pub(crate) const fn new(role: PublicationMemberRole, relative_path: PathBuf) -> Self {
        Self {
            role,
            relative_path,
        }
    }

    pub(crate) const fn role(&self) -> PublicationMemberRole {
        self.role
    }

    pub(crate) fn relative_path(&self) -> &Path {
        &self.relative_path
    }
}

impl GenerationPublicationPlan {
    pub(crate) fn prepare_initial(
        root: &Path,
        staging: &CompleteStagingSet,
        target_control: ControlRecord,
    ) -> FormatResult<Self> {
        validate_initial_control(staging, target_control)?;
        let mut source = GenerationFileSource::for_initial_publication(root, staging.path());
        let validated =
            validate_control_generation(target_control, &mut source).map_err(|error| {
                FormatError::InvalidPublicationGraph {
                    detail: error.to_string(),
                }
            })?;
        drop(source);
        if !validated.unavailable_indexes().is_empty() {
            return invalid("initial generation contains an unavailable referenced index");
        }
        let snapshot = PhysicalGenerationSnapshot::from_validated(&validated)?;
        if !snapshot.database_manifest().tables().is_empty()
            || !snapshot.artifact_references().is_empty()
        {
            return invalid("initial generation is not an empty bootstrap generation");
        }
        let members = derive_initial_members(&snapshot);
        let member_paths = members
            .iter()
            .map(|member| member.relative_path.clone())
            .collect::<Vec<_>>();
        staging.validate_selected_members(
            root,
            &member_paths,
            StagingDiscoveryLimits::default(),
        )?;
        Ok(Self {
            // There is no predecessor for generation one. Keeping the target
            // here makes the field total without inventing a generation-zero
            // CONTROL identity; initial publication never reads this value.
            expected_control: target_control,
            target_control,
            control_bytes: encode_control_slot(target_control),
            snapshot,
            members,
        })
    }

    pub(crate) fn prepare(
        root: &Path,
        staging: &CompleteStagingSet,
        current: &PhysicalGenerationSnapshot,
        target_control: ControlRecord,
    ) -> FormatResult<Self> {
        validate_control_transition(current.control(), staging, target_control)?;
        let mut source = GenerationFileSource::for_publication(
            root,
            staging.path(),
            current.artifact_references(),
        );
        let validated =
            validate_control_generation(target_control, &mut source).map_err(|error| {
                FormatError::InvalidPublicationGraph {
                    detail: error.to_string(),
                }
            })?;
        drop(source);
        let snapshot = PhysicalGenerationSnapshot::from_validated(&validated)?;
        let current_artifacts = current_artifacts(current)?;
        validate_unavailable_inherited_indexes(
            &current_artifacts,
            &snapshot,
            validated.unavailable_indexes(),
        )?;
        let members = derive_members(current, &snapshot, &current_artifacts)?;
        let member_paths = members
            .iter()
            .map(|member| member.relative_path.clone())
            .collect::<Vec<_>>();
        staging.validate_selected_members(
            root,
            &member_paths,
            StagingDiscoveryLimits::default(),
        )?;
        Ok(Self {
            expected_control: current.control(),
            target_control,
            control_bytes: encode_control_slot(target_control),
            snapshot,
            members,
        })
    }

    /// Prepare a compaction generation whose expensive immutable outputs were
    /// already promoted to their final content-addressed locators while the
    /// publication fence is held. The completed staging set therefore owns
    /// only the bounded manifest members (and an optional tombstone artifact),
    /// not the prebuilt DATA/INDEX payloads.
    pub(crate) fn prepare_rebased_compaction(
        root: &Path,
        staging: &CompleteStagingSet,
        current: &PhysicalGenerationSnapshot,
        target_control: ControlRecord,
        prepublished_artifacts: &[ArtifactRef],
    ) -> FormatResult<Self> {
        validate_control_transition(current.control(), staging, target_control)?;
        let validated_artifacts = current
            .artifact_references()
            .into_iter()
            .chain(prepublished_artifacts.iter().copied());
        let mut source =
            GenerationFileSource::for_publication(root, staging.path(), validated_artifacts);
        let validated =
            validate_control_generation(target_control, &mut source).map_err(|error| {
                FormatError::InvalidPublicationGraph {
                    detail: error.to_string(),
                }
            })?;
        drop(source);
        let snapshot = PhysicalGenerationSnapshot::from_validated(&validated)?;
        let current_artifacts = current_artifacts(current)?;
        validate_unavailable_inherited_indexes(
            &current_artifacts,
            &snapshot,
            validated.unavailable_indexes(),
        )?;
        let mut members = derive_members(current, &snapshot, &current_artifacts)?;

        let target_artifacts = snapshot
            .artifact_references()
            .into_iter()
            .collect::<HashSet<_>>();
        let current_references = current
            .artifact_references()
            .into_iter()
            .collect::<HashSet<_>>();
        let mut prepublished_by_path = HashMap::new();
        for reference in prepublished_artifacts {
            if current_references.contains(reference) {
                return invalid("prepublished compaction artifact is already current");
            }
            if !target_artifacts.contains(reference) {
                return invalid("prepublished compaction artifact is absent from target");
            }
            if prepublished_by_path
                .insert(reference.relative_path(), *reference)
                .is_some()
            {
                return invalid("prepublished compaction artifacts repeat a locator");
            }
        }

        let mut matched_prepublished = HashSet::new();
        let mut staged_member_paths = Vec::with_capacity(members.len());
        for member in &members {
            let Some(reference) = prepublished_by_path.get(member.relative_path()) else {
                staged_member_paths.push(member.relative_path.clone());
                continue;
            };
            let role_matches = matches!(
                (member.role(), reference.kind()),
                (PublicationMemberRole::Data, ArtifactKind::Data)
                    | (PublicationMemberRole::Index, ArtifactKind::Index)
            );
            if !role_matches {
                return invalid("prepublished artifact has the wrong publication role");
            }
            matched_prepublished.insert(*reference);
        }
        if matched_prepublished.len() != prepublished_artifacts.len() {
            return invalid("prepublished compaction artifact has no publication member");
        }
        members.retain(|member| !prepublished_by_path.contains_key(member.relative_path()));
        staging.validate_selected_members(
            root,
            &staged_member_paths,
            StagingDiscoveryLimits::default(),
        )?;
        Ok(Self {
            expected_control: current.control(),
            target_control,
            control_bytes: encode_control_slot(target_control),
            snapshot,
            members,
        })
    }

    pub(crate) const fn expected_control(&self) -> ControlRecord {
        self.expected_control
    }

    pub(crate) const fn target_control(&self) -> ControlRecord {
        self.target_control
    }

    pub(crate) const fn control_bytes(&self) -> &[u8; CONTROL_RECORD_BYTES] {
        &self.control_bytes
    }

    pub(crate) const fn snapshot(&self) -> &PhysicalGenerationSnapshot {
        &self.snapshot
    }

    pub(crate) fn members(&self) -> &[PublicationMember] {
        &self.members
    }
}

fn validate_initial_control(
    staging: &CompleteStagingSet,
    target: ControlRecord,
) -> FormatResult<()> {
    if target.slot() != ControlSlotIndex::Zero
        || target.database_generation().get() != 1
        || target.database_manifest().generation().get() != 1
        || target.catalog().generation().get() != 1
        || target.wal_replay_floor().generation().get() != 1
        || target.wal_replay_floor().lsn() != 0
    {
        return invalid("initial CONTROL does not describe generation one at WAL floor 1:0");
    }
    if staging.owner().intended_generation() != target.database_generation()
        || staging.complete().intended_generation() != target.database_generation()
        || staging.owner().writer_instance_id() != target.writer_instance_id()
        || staging.complete().writer_instance_id() != target.writer_instance_id()
    {
        return invalid("initial CONTROL identity differs from staging ownership");
    }
    Ok(())
}

fn derive_initial_members(snapshot: &PhysicalGenerationSnapshot) -> Vec<PublicationMember> {
    vec![
        PublicationMember::new(
            PublicationMemberRole::CatalogPack,
            catalog_path(snapshot.database_manifest().catalog()),
        ),
        PublicationMember::new(
            PublicationMemberRole::WalSuccessor,
            wal_path(snapshot.control().wal_replay_floor().generation()),
        ),
        PublicationMember::new(
            PublicationMemberRole::DatabaseManifest,
            database_manifest_path(snapshot.control().database_manifest()),
        ),
    ]
}

fn validate_control_transition(
    current: ControlRecord,
    staging: &CompleteStagingSet,
    target: ControlRecord,
) -> FormatResult<()> {
    if target.database_id() != current.database_id() {
        return invalid("target CONTROL belongs to another database");
    }
    if target.database_generation() != current.database_generation().checked_next()? {
        return invalid("target CONTROL is not the next database generation");
    }
    if target.slot() != inactive_slot(current.slot()) {
        return invalid("target CONTROL does not use the inactive slot");
    }
    if staging.owner().intended_generation() != target.database_generation()
        || staging.complete().intended_generation() != target.database_generation()
    {
        return invalid("staging set targets another database generation");
    }
    if target.writer_instance_id() != staging.owner().writer_instance_id()
        || target.writer_instance_id() != staging.complete().writer_instance_id()
    {
        return invalid("CONTROL writer differs from staging owner");
    }
    if target.database_manifest().id() == current.database_manifest().id() {
        return invalid("successor generation reuses the database-manifest ID");
    }
    if target.catalog().generation() < current.catalog().generation() {
        return invalid("catalog generation regresses");
    }
    if target.catalog().generation() == current.catalog().generation()
        && target.catalog() != current.catalog()
    {
        return invalid("catalog identity changes without advancing its generation");
    }
    let current_floor = current.wal_replay_floor();
    let target_floor = target.wal_replay_floor();
    if target_floor.generation() < current_floor.generation()
        || (target_floor.generation() == current_floor.generation()
            && target_floor.lsn() < current_floor.lsn())
    {
        return invalid("WAL replay floor regresses");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct CurrentArtifact {
    reference: ArtifactRef,
    index_source_data: Option<ArtifactRef>,
}

fn current_artifacts(
    current: &PhysicalGenerationSnapshot,
) -> FormatResult<HashMap<ArtifactId, CurrentArtifact>> {
    let mut artifacts = HashMap::new();
    for manifest in current.table_manifests() {
        for segment in manifest.segments() {
            let data = segment.data_artifact();
            if artifacts
                .insert(
                    data.id(),
                    CurrentArtifact {
                        reference: data,
                        index_source_data: None,
                    },
                )
                .is_some()
            {
                return invalid("current generation contains a duplicate artifact ID");
            }
            if let Some(index) = segment.index_artifact() {
                if artifacts
                    .insert(
                        index.id(),
                        CurrentArtifact {
                            reference: index,
                            index_source_data: Some(data),
                        },
                    )
                    .is_some()
                {
                    return invalid("current generation contains a duplicate artifact ID");
                }
            }
        }
    }
    Ok(artifacts)
}

fn validate_unavailable_inherited_indexes(
    current_artifacts: &HashMap<ArtifactId, CurrentArtifact>,
    target: &PhysicalGenerationSnapshot,
    unavailable_indexes: &[UnavailableIndex],
) -> FormatResult<()> {
    let mut unavailable = unavailable_indexes
        .iter()
        .map(|index| (index.reference().id(), index.reference()))
        .collect::<HashMap<_, _>>();
    if unavailable.len() != unavailable_indexes.len() {
        return invalid("target generation reports a duplicate unavailable index");
    }

    for manifest in target.table_manifests() {
        for segment in manifest.segments() {
            let Some(index) = segment.index_artifact() else {
                continue;
            };
            let Some(unavailable_reference) = unavailable.remove(&index.id()) else {
                continue;
            };
            let Some(current) = current_artifacts.get(&index.id()) else {
                return invalid("target generation contains a new unavailable index");
            };
            if unavailable_reference != index
                || current.reference != index
                || current.index_source_data != Some(segment.data_artifact())
            {
                return invalid("target generation changes an unavailable inherited index pair");
            }
        }
    }

    if !unavailable.is_empty() {
        return invalid("target unavailable index is absent from table manifests");
    }
    Ok(())
}

fn derive_members(
    current: &PhysicalGenerationSnapshot,
    target: &PhysicalGenerationSnapshot,
    current_artifacts: &HashMap<ArtifactId, CurrentArtifact>,
) -> FormatResult<Vec<PublicationMember>> {
    let mut data = Vec::new();
    let mut indexes = Vec::new();
    for reference in target.artifact_references() {
        match current_artifacts.get(&reference.id()) {
            Some(existing) if existing.reference == reference => continue,
            Some(_) => return invalid("target reuses an artifact ID with different metadata"),
            None => match reference.kind() {
                ArtifactKind::Data => data.push(PublicationMember::new(
                    PublicationMemberRole::Data,
                    reference.relative_path(),
                )),
                ArtifactKind::Index => indexes.push(PublicationMember::new(
                    PublicationMemberRole::Index,
                    reference.relative_path(),
                )),
            },
        }
    }

    let current_tables = current
        .database_manifest()
        .tables()
        .iter()
        .copied()
        .map(|reference| (reference.table_id(), reference))
        .collect::<HashMap<_, _>>();
    let current_manifest_ids = current
        .database_manifest()
        .tables()
        .iter()
        .map(|reference| (reference.manifest().id(), *reference))
        .collect::<HashMap<ManifestId, TableManifestRef>>();
    let mut tables = Vec::new();
    for reference in target.database_manifest().tables() {
        if current_tables.get(&reference.table_id()) == Some(reference) {
            continue;
        }
        if current_manifest_ids.contains_key(&reference.manifest().id()) {
            return invalid("target reuses a table-manifest ID with different metadata");
        }
        if reference.manifest().generation().get() != target.control().database_generation().get() {
            return invalid("changed table manifest is not owned by target generation");
        }
        tables.push(PublicationMember::new(
            PublicationMemberRole::TableManifest,
            table_manifest_path(*reference),
        ));
    }

    let current_catalog = current.database_manifest().catalog();
    let target_catalog = target.database_manifest().catalog();
    let catalog = if target_catalog == current_catalog {
        None
    } else {
        if target_catalog.id() == current_catalog.id()
            || target_catalog.generation() <= current_catalog.generation()
        {
            return invalid("changed catalog does not have a fresh identity and generation");
        }
        Some(PublicationMember::new(
            PublicationMemberRole::CatalogPack,
            catalog_path(target_catalog),
        ))
    };

    let wal = (target.control().wal_replay_floor().generation()
        != current.control().wal_replay_floor().generation())
    .then(|| {
        PublicationMember::new(
            PublicationMemberRole::WalSuccessor,
            wal_path(target.control().wal_replay_floor().generation()),
        )
    });

    data.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    indexes.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    tables.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut members = Vec::with_capacity(
        data.len() + indexes.len() + tables.len() + usize::from(catalog.is_some()) + 2,
    );
    members.extend(data);
    members.extend(indexes);
    members.extend(tables);
    if let Some(catalog) = catalog {
        members.push(catalog);
    }
    if let Some(wal) = wal {
        members.push(wal);
    }
    members.push(PublicationMember::new(
        PublicationMemberRole::DatabaseManifest,
        database_manifest_path(target.control().database_manifest()),
    ));

    let unique = members
        .iter()
        .map(|member| member.relative_path.clone())
        .collect::<BTreeSet<_>>();
    if unique.len() != members.len() {
        return invalid("target generation maps distinct members to one locator");
    }
    Ok(members)
}

const fn inactive_slot(slot: ControlSlotIndex) -> ControlSlotIndex {
    match slot {
        ControlSlotIndex::Zero => ControlSlotIndex::One,
        ControlSlotIndex::One => ControlSlotIndex::Zero,
    }
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidPublication { detail })
}
