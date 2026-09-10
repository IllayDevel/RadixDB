use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use radixdb_catalog::ObjectId;

use crate::mvcc::FileLock;

#[cfg(any(test, feature = "test-failpoints"))]
use std::sync::atomic::AtomicU64;

use super::super::{
    decode_control_slot, encode_control_slot, encode_database_manifest, encode_table_manifest,
    encoded_database_manifest_length, fault::reach_generation_boundary, ArtifactCleanupLimits,
    ArtifactCleanupReport, ArtifactGarbageCollector, ArtifactReachability, ArtifactRef,
    CompleteStagingSet, ControlRecord, ControlSlotIndex, DatabaseId, DatabaseManifest,
    DatabaseManifestRootRef, FormatError, FormatResult, GenerationCrashPoint, ImmutableMemberRef,
    ManifestGeneration, ManifestId, ManifestKind, ManifestRef, ReachabilityLimits,
    SegmentDescriptor, SegmentId, StagingOwner, TableManifest, TableManifestRef,
    ValidatedGeneration, WriterInstanceId, CONTROL_RECORD_BYTES,
};

const AUTOMATIC_RETIREMENT_GENERATION_INTERVAL: u64 = 16;
use super::diagnostics::{PublicationFenceKind, PublicationFenceTimer};
use super::filesystem::{
    publish_members, publish_prebuilt_artifacts, validate_prebuilt_artifacts,
    GenerationPublicationPlan,
};
use super::rebuild::StagedIndexReplacement;
use super::{
    maintenance::{validate_maintenance_transition, validate_rebased_compaction_transition},
    CheckpointOutcome, FrozenCheckpoint, FrozenMaintenance, MaintenanceOutcome,
};
use super::{
    ActiveLeaseSet, ArtifactBuildKind, ArtifactBuildLease, LeaseLimits, LeaseRegistry,
    PhysicalGenerationLease,
};

#[derive(Debug, Clone)]
pub struct PhysicalGenerationSnapshot {
    control: ControlRecord,
    database_manifest: DatabaseManifest,
    table_manifests: Vec<TableManifest>,
}

impl PhysicalGenerationSnapshot {
    pub fn new(
        control: ControlRecord,
        database_manifest: DatabaseManifest,
        mut table_manifests: Vec<TableManifest>,
    ) -> FormatResult<Self> {
        validate_root(control, &database_manifest)?;
        table_manifests.sort_unstable_by_key(TableManifest::table_id);
        if table_manifests.len() != database_manifest.tables().len() {
            return Err(invalid("runtime table set differs from database manifest"));
        }
        for (manifest, reference) in table_manifests.iter().zip(database_manifest.tables()) {
            if manifest.database_id() != database_manifest.database_id()
                || manifest.table_id() != reference.table_id()
                || manifest.manifest_id() != reference.manifest().id()
                || manifest.generation() != reference.manifest().generation()
                || manifest.catalog_generation().get()
                    > database_manifest.catalog().generation().get()
            {
                return Err(invalid(
                    "runtime table manifest differs from its database-manifest reference",
                ));
            }
        }
        Ok(Self {
            control,
            database_manifest,
            table_manifests,
        })
    }

    pub fn from_validated(generation: &ValidatedGeneration) -> FormatResult<Self> {
        Self::new(
            generation.control(),
            generation.database_manifest().clone(),
            generation.table_manifests().to_vec(),
        )
    }

    pub const fn control(&self) -> ControlRecord {
        self.control
    }

    pub const fn database_manifest(&self) -> &DatabaseManifest {
        &self.database_manifest
    }

    pub fn table_manifests(&self) -> &[TableManifest] {
        &self.table_manifests
    }

    pub fn table_manifest(&self, table_id: ObjectId) -> Option<&TableManifest> {
        self.table_manifests
            .binary_search_by_key(&table_id, TableManifest::table_id)
            .ok()
            .map(|index| &self.table_manifests[index])
    }

    pub fn segment(&self, table_id: ObjectId, segment_id: SegmentId) -> Option<SegmentDescriptor> {
        self.table_manifest(table_id).and_then(|manifest| {
            manifest
                .segments()
                .binary_search_by_key(&segment_id, |segment| segment.id())
                .ok()
                .map(|index| manifest.segments()[index])
        })
    }

    pub fn artifact_references(&self) -> Vec<ArtifactRef> {
        self.table_manifests
            .iter()
            .flat_map(|manifest| manifest.segments())
            .flat_map(|segment| {
                std::iter::once(segment.data_artifact()).chain(segment.index_artifact())
            })
            .collect()
    }

    pub(crate) fn immutable_member_references(&self) -> FormatResult<Vec<ImmutableMemberRef>> {
        let mut members =
            Vec::with_capacity(2 + self.table_manifests.len() + self.artifact_references().len());
        members.push(ImmutableMemberRef::database_manifest(
            self.control.database_manifest(),
            encoded_database_manifest_length(&self.database_manifest)?,
        )?);
        members.push(ImmutableMemberRef::Catalog(
            self.database_manifest.catalog(),
        ));
        members.extend(
            self.database_manifest
                .tables()
                .iter()
                .copied()
                .map(ImmutableMemberRef::TableManifest),
        );
        members.extend(
            self.artifact_references()
                .into_iter()
                .map(ImmutableMemberRef::Artifact),
        );
        Ok(members)
    }
}

pub struct PhysicalGenerationPublisher {
    filesystem: Option<PublisherFilesystemOwner>,
    current: ArcSwap<PhysicalGenerationSnapshot>,
    publication_lock: Mutex<()>,
    retirement_lock: Mutex<()>,
    automatic_retirement_source: Mutex<Option<Arc<PhysicalGenerationSnapshot>>>,
    automatic_retirement_requested: AtomicBool,
    automatic_retirement_initialized: AtomicBool,
    leases: Arc<LeaseRegistry>,
    #[cfg(any(test, feature = "test-failpoints"))]
    interleave_scope: u64,
}

struct PublisherFilesystemOwner {
    root: PathBuf,
    database_id: DatabaseId,
    writer_lock: Mutex<Option<FileLock>>,
}

#[cfg(any(test, feature = "test-failpoints"))]
static NEXT_INTERLEAVE_SCOPE: AtomicU64 = AtomicU64::new(1);

impl PhysicalGenerationPublisher {
    /// Open the sole filesystem publication owner for one recovered database.
    ///
    /// This is intentionally the only public constructor: a publisher without
    /// the exact root and writer lock cannot expose filesystem mutation APIs.
    pub fn open(
        database_root: impl AsRef<Path>,
        snapshot: PhysicalGenerationSnapshot,
    ) -> FormatResult<Self> {
        Self::open_with_lease_limits(database_root, snapshot, LeaseLimits::default())
    }

    pub fn open_with_lease_limits(
        database_root: impl AsRef<Path>,
        snapshot: PhysicalGenerationSnapshot,
        limits: LeaseLimits,
    ) -> FormatResult<Self> {
        let writer_lock = FileLock::acquire(database_root.as_ref()).map_err(|_| {
            FormatError::InvalidFilesystemOwner {
                detail: "database writer lock could not be acquired",
            }
        })?;
        Self::from_locked_with_lease_limits(snapshot, writer_lock, limits)
    }

    pub(crate) fn from_locked(
        snapshot: PhysicalGenerationSnapshot,
        writer_lock: FileLock,
    ) -> FormatResult<Self> {
        Self::from_locked_with_lease_limits(snapshot, writer_lock, LeaseLimits::default())
    }

    pub(crate) fn from_locked_with_lease_limits(
        snapshot: PhysicalGenerationSnapshot,
        writer_lock: FileLock,
        limits: LeaseLimits,
    ) -> FormatResult<Self> {
        let root = writer_lock.root().to_path_buf();
        validate_lock_root(&writer_lock, &root)?;
        let database_id = snapshot.control().database_id();
        validate_control_owner(&root, snapshot.control())?;
        Ok(Self {
            filesystem: Some(PublisherFilesystemOwner {
                root,
                database_id,
                writer_lock: Mutex::new(Some(writer_lock)),
            }),
            current: ArcSwap::from_pointee(snapshot),
            publication_lock: Mutex::new(()),
            retirement_lock: Mutex::new(()),
            automatic_retirement_source: Mutex::new(None),
            automatic_retirement_requested: AtomicBool::new(false),
            automatic_retirement_initialized: AtomicBool::new(false),
            leases: LeaseRegistry::new(limits),
            #[cfg(any(test, feature = "test-failpoints"))]
            interleave_scope: NEXT_INTERLEAVE_SCOPE.fetch_add(1, Ordering::Relaxed),
        })
    }

    pub fn open_validated(
        database_root: impl AsRef<Path>,
        generation: &ValidatedGeneration,
    ) -> FormatResult<Self> {
        Self::open(
            database_root,
            PhysicalGenerationSnapshot::from_validated(generation)?,
        )
    }

    #[cfg(test)]
    pub(crate) fn for_runtime_tests(snapshot: PhysicalGenerationSnapshot) -> Self {
        Self::for_runtime_tests_with_lease_limits(snapshot, LeaseLimits::default())
    }

    #[cfg(test)]
    pub(crate) fn for_runtime_tests_with_lease_limits(
        snapshot: PhysicalGenerationSnapshot,
        limits: LeaseLimits,
    ) -> Self {
        Self {
            filesystem: None,
            current: ArcSwap::from_pointee(snapshot),
            publication_lock: Mutex::new(()),
            retirement_lock: Mutex::new(()),
            automatic_retirement_source: Mutex::new(None),
            automatic_retirement_requested: AtomicBool::new(false),
            automatic_retirement_initialized: AtomicBool::new(false),
            leases: LeaseRegistry::new(limits),
            interleave_scope: NEXT_INTERLEAVE_SCOPE.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn pin(&self) -> FormatResult<PhysicalGenerationLease> {
        // GC takes the same narrow lock while snapshotting current + leases.
        // Publication has its own wider fence and never retires files itself.
        let _retirement_guard = self.retirement_lock.lock();
        let snapshot = self.current.load_full();
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::interleave_scoped(
            self.interleave_scope,
            crate::test_failpoints::InterleavePoint::GenerationPinLoaded,
            snapshot.control().database_generation().get() as i64,
        );
        self.leases.register_generation(snapshot)
    }

    /// Process-local identity used only by deterministic scheduling tests.
    #[cfg(any(test, feature = "test-failpoints"))]
    #[doc(hidden)]
    pub const fn test_interleave_scope(&self) -> u64 {
        self.interleave_scope
    }

    pub fn begin_artifact_build(
        &self,
        kind: ArtifactBuildKind,
        source: &PhysicalGenerationLease,
        staging_owner: StagingOwner,
    ) -> FormatResult<ArtifactBuildLease> {
        self.leases.register_build(kind, source, staging_owner)
    }

    pub fn active_leases(&self) -> ActiveLeaseSet {
        self.leases.active()
    }

    /// Revoke filesystem mutation authority before the owning engine releases
    /// its final writer lock. Retained publisher clones remain valid for
    /// immutable reads, but every later publication attempt fails closed.
    pub(crate) fn release_writer_lock(&self) {
        if let Some(owner) = &self.filesystem {
            *owner.writer_lock.lock() = None;
        }
    }

    /// Run one bounded artifact-GC generation while physical publication is
    /// fenced. The current CONTROL graph, validated fallback roots, retained
    /// snapshot members and a strong lease snapshot form the complete proof.
    pub fn collect_unreachable_artifacts(
        &self,
        collector: &ArtifactGarbageCollector,
        fallback_roots: &[PhysicalGenerationSnapshot],
        retained_snapshot_artifacts: &[ArtifactRef],
        now_unix_ns: u64,
        reachability_limits: ReachabilityLimits,
    ) -> FormatResult<ArtifactCleanupReport> {
        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Cleanup);
        let expected = self.current.load_full();
        self.validate_external_root(collector.root())?;
        self.validate_filesystem_owner(expected.control())?;
        let (current, leases) = {
            // This is the ordering point paired with pin(): after it, a new
            // pin can only observe `current`, which is already a GC root.
            let _retirement_guard = self.retirement_lock.lock();
            let current = self.current.load_full();
            if fallback_roots
                .iter()
                .any(|root| root.control().database_id() != current.control().database_id())
            {
                return Err(FormatError::InvalidCleanup {
                    detail: "fallback CONTROL root belongs to another database",
                });
            }
            (current, self.leases.active())
        };
        let mut roots = Vec::with_capacity(fallback_roots.len() + 1);
        roots.push((*current).clone());
        roots.extend_from_slice(fallback_roots);
        let reachable = ArtifactReachability::build(
            &roots,
            retained_snapshot_artifacts,
            &leases,
            reachability_limits,
        )?;
        reach_generation_boundary(GenerationCrashPoint::GcAfterRootSnapshot).map_err(|error| {
            FormatError::CleanupIo {
                operation: "inject after cleanup root snapshot",
                kind: error.kind(),
            }
        })?;
        collector.run_cycle(&reachable, now_unix_ns)
    }

    pub fn publish_index_replacement(
        &self,
        prepared: PreparedIndexReplacement,
        sink: &mut impl IndexReplacementPublicationSink,
    ) -> FormatResult<PhysicalGenerationLease> {
        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        if current.control() != prepared.expected_control {
            return Err(invalid(
                "index replacement was prepared from a stale physical generation",
            ));
        }
        self.validate_external_root(sink.database_root())?;
        self.validate_filesystem_owner(current.control())?;
        self.validate_staging_path(prepared.staged.path())?;

        let snapshot = Arc::new(prepared.snapshot);
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        sink.publish_staged_index_and_sync(&prepared.staged)?;
        sink.publish_table_manifest_and_sync(
            prepared.table_reference,
            &prepared.table_manifest_bytes,
        )?;
        sink.publish_database_manifest_and_sync(
            prepared.database_reference,
            &prepared.database_manifest_bytes,
        )?;
        sink.publish_control_and_sync(prepared.control.slot(), &prepared.control_bytes)?;

        publish_runtime(&self.current, &snapshot)?;
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(lease)
    }

    /// Atomically publish one complete physical generation from a durable
    /// staging set. Immutable members become reachable only when the inactive
    /// CONTROL slot is durable; the runtime pointer changes afterwards.
    pub fn publish_staged_generation(
        &self,
        build: ArtifactBuildLease,
        staging: CompleteStagingSet,
        target_control: ControlRecord,
    ) -> FormatResult<PhysicalGenerationLease> {
        let source = self.current.load_full();
        let root = self.validate_filesystem_owner(source.control())?;
        self.validate_staging_path(staging.path())?;
        build.validate_publication(&source, &staging, Some(ArtifactBuildKind::Seal))?;
        let plan = GenerationPublicationPlan::prepare(root, &staging, &source, target_control)?;

        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        let root = self.validate_filesystem_owner(current.control())?;
        if current.control() != plan.expected_control() {
            return Err(FormatError::InvalidLease {
                detail: "artifact build source differs from publication source",
            });
        }
        let snapshot = Arc::new(plan.snapshot().clone());
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        publish_members(root, &staging, &plan)?;
        publish_runtime(&self.current, &snapshot)?;
        staging.retire_published_best_effort();
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(lease)
    }

    /// Atomically publish a catalog-changing DDL generation whose immutable
    /// members may include replacement INDEX packs for already sealed DATA.
    /// This is distinct from rebuild maintenance: the catalog generation is
    /// deliberately allowed to advance with the new definitions.
    pub fn publish_ddl_generation(
        &self,
        build: ArtifactBuildLease,
        staging: CompleteStagingSet,
        target_control: ControlRecord,
    ) -> FormatResult<PhysicalGenerationLease> {
        let source = self.current.load_full();
        let root = self.validate_filesystem_owner(source.control())?;
        self.validate_staging_path(staging.path())?;
        build.validate_publication(&source, &staging, Some(ArtifactBuildKind::DdlPublication))?;
        let plan = GenerationPublicationPlan::prepare(root, &staging, &source, target_control)?;

        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        let root = self.validate_filesystem_owner(current.control())?;
        if current.control() != plan.expected_control() {
            return Err(FormatError::InvalidLease {
                detail: "DDL artifact build source differs from publication source",
            });
        }
        let snapshot = Arc::new(plan.snapshot().clone());
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        publish_members(root, &staging, &plan)?;
        publish_runtime(&self.current, &snapshot)?;
        staging.retire_published_best_effort();
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(lease)
    }

    /// Publish one frozen checkpoint and only then attempt bounded retirement
    /// of WAL generations excluded by both retained CONTROL roots.
    pub fn publish_checkpoint(
        &self,
        checkpoint: FrozenCheckpoint,
    ) -> FormatResult<CheckpointOutcome> {
        let (expected_control, target_control, staging, build, obsolete_wal) =
            checkpoint.into_parts();
        let source = self.current.load_full();
        let root = self.validate_filesystem_owner(source.control())?;
        self.validate_staging_path(staging.path())?;
        if source.control() != expected_control {
            return Err(FormatError::InvalidCheckpoint {
                detail: "checkpoint boundary is stale",
            });
        }
        build.validate_publication(&source, &staging, Some(ArtifactBuildKind::Seal))?;
        let plan = GenerationPublicationPlan::prepare(root, &staging, &source, target_control)?;

        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        let root = self.validate_filesystem_owner(current.control())?;
        if current.control() != expected_control {
            return Err(FormatError::InvalidCheckpoint {
                detail: "checkpoint boundary is stale",
            });
        }
        let snapshot = Arc::new(plan.snapshot().clone());
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        publish_members(root, &staging, &plan)?;
        publish_runtime(&self.current, &snapshot)?;
        staging.retire_published_best_effort();
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(CheckpointOutcome::finish(
            lease,
            root,
            expected_control,
            target_control,
            obsolete_wal,
        ))
    }

    /// Publish compaction or accelerator rebuild as an independent physical
    /// generation that cannot advance catalog or WAL checkpoint authority.
    pub fn publish_maintenance(
        &self,
        maintenance: FrozenMaintenance,
    ) -> FormatResult<MaintenanceOutcome> {
        let (kind, expected_control, target_control, staging, build) = maintenance.into_parts();
        let source = self.current.load_full();
        let root = self.validate_filesystem_owner(source.control())?;
        self.validate_staging_path(staging.path())?;
        if source.control() != expected_control {
            return Err(FormatError::InvalidMaintenance {
                detail: "maintenance boundary is stale",
            });
        }
        let expected_kind = match kind {
            super::MaintenanceKind::Compaction => ArtifactBuildKind::Compaction,
            super::MaintenanceKind::IndexRebuild => ArtifactBuildKind::IndexRebuild,
        };
        build.validate_publication(&source, &staging, Some(expected_kind))?;
        let plan = GenerationPublicationPlan::prepare(root, &staging, &source, target_control)?;
        let retired_artifacts = validate_maintenance_transition(kind, &source, plan.snapshot())?;

        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        let root = self.validate_filesystem_owner(current.control())?;
        if current.control() != expected_control {
            return Err(FormatError::InvalidMaintenance {
                detail: "maintenance boundary is stale",
            });
        }
        let snapshot = Arc::new(plan.snapshot().clone());
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        publish_members(root, &staging, &plan)?;
        publish_runtime(&self.current, &snapshot)?;
        staging.retire_published_best_effort();
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(MaintenanceOutcome::new(kind, lease, retired_artifacts))
    }

    /// Publish immutable compaction outputs against the newest compatible
    /// physical generation. Expensive DATA/INDEX bodies were completed by the
    /// original optimistic build; only bounded manifests are rebuilt from the
    /// current generation before this call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_rebased_compaction(
        &self,
        prebuilt_staging: CompleteStagingSet,
        prebuilt_build: ArtifactBuildLease,
        prebuilt_artifacts: &[ArtifactRef],
        maintenance: FrozenMaintenance,
        table_id: ObjectId,
        removals: &[SegmentDescriptor],
        additions: &[SegmentDescriptor],
    ) -> FormatResult<MaintenanceOutcome> {
        let (kind, expected_control, target_control, staging, build) = maintenance.into_parts();
        if kind != super::MaintenanceKind::Compaction {
            return Err(FormatError::InvalidMaintenance {
                detail: "rebased publication is not a compaction",
            });
        }
        let expected_kind = ArtifactBuildKind::Compaction;
        let prebuilt_source = prebuilt_build.source().snapshot();
        if prebuilt_source.control().database_id() != expected_control.database_id()
            || prebuilt_source.control().catalog() != expected_control.catalog()
        {
            return Err(FormatError::InvalidMaintenance {
                detail: "prebuilt compaction source is not catalog-compatible",
            });
        }
        prebuilt_build.validate_publication(
            prebuilt_source,
            &prebuilt_staging,
            Some(expected_kind),
        )?;
        let source = self.current.load_full();
        let root = self.validate_filesystem_owner(source.control())?;
        self.validate_staging_path(prebuilt_staging.path())?;
        self.validate_staging_path(staging.path())?;
        validate_prebuilt_artifacts(root, &prebuilt_staging, prebuilt_artifacts)?;

        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Generation);
        let current = self.current.load_full();
        let root = self.validate_filesystem_owner(current.control())?;
        if current.control() != expected_control {
            return Err(FormatError::InvalidMaintenance {
                detail: "maintenance boundary is stale",
            });
        }
        build.validate_publication(&current, &staging, Some(expected_kind))?;

        // These locators are content-addressed and not yet reachable from
        // CONTROL. Keep the publication fence until the rebased manifest graph
        // either commits them atomically or leaves safe GC candidates.
        publish_prebuilt_artifacts(root, &prebuilt_staging, prebuilt_artifacts)?;
        let plan = GenerationPublicationPlan::prepare_rebased_compaction(
            root,
            &staging,
            &current,
            target_control,
            prebuilt_artifacts,
        )?;
        let retired_artifacts = validate_rebased_compaction_transition(
            &current,
            plan.snapshot(),
            table_id,
            removals,
            additions,
        )?;
        let snapshot = Arc::new(plan.snapshot().clone());
        let lease = self.leases.register_generation(Arc::clone(&snapshot))?;
        publish_members(root, &staging, &plan)?;
        publish_runtime(&self.current, &snapshot)?;
        prebuilt_staging.retire_published_best_effort();
        staging.retire_published_best_effort();
        self.schedule_retired_member_collection(&current, &snapshot);
        Ok(MaintenanceOutcome::new(kind, lease, retired_artifacts))
    }

    fn schedule_retired_member_collection(
        &self,
        fallback: &Arc<PhysicalGenerationSnapshot>,
        current: &Arc<PhysicalGenerationSnapshot>,
    ) {
        *self.automatic_retirement_source.lock() = Some(Arc::clone(fallback));
        let first_publication = !self
            .automatic_retirement_initialized
            .swap(true, Ordering::AcqRel);
        if first_publication
            || current
                .control()
                .database_generation()
                .get()
                .is_multiple_of(AUTOMATIC_RETIREMENT_GENERATION_INTERVAL)
        {
            self.automatic_retirement_requested
                .store(true, Ordering::Release);
        }
    }

    /// Run an automatically scheduled retirement cycle outside the foreground
    /// publication that made old immutable members unreachable.
    ///
    /// The background owner still takes the normal publication/retirement
    /// fences and rebuilds exact reachability from the newest current and
    /// fallback generations. A failed cycle remains requested for retry.
    pub fn run_scheduled_retirement(&self) -> FormatResult<Option<ArtifactCleanupReport>> {
        if !self.automatic_retirement_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(owner) = &self.filesystem else {
            self.automatic_retirement_requested
                .store(false, Ordering::Release);
            return Ok(None);
        };
        let _guard = self.publication_lock.lock();
        let _fence_timer = PublicationFenceTimer::start(PublicationFenceKind::Cleanup);
        if !self.automatic_retirement_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        let expected = self.current.load_full();
        self.validate_filesystem_owner(expected.control())?;
        let fallback =
            self.automatic_retirement_source
                .lock()
                .clone()
                .ok_or(FormatError::InvalidCleanup {
                    detail: "scheduled retirement has no retained fallback generation",
                })?;
        let (current, leases) = {
            let _retirement_guard = self.retirement_lock.lock();
            (self.current.load_full(), self.leases.active())
        };
        if fallback.control().database_id() != current.control().database_id() {
            return Err(FormatError::InvalidCleanup {
                detail: "scheduled fallback CONTROL belongs to another database",
            });
        }
        let roots = [(*current).clone(), (*fallback).clone()];
        let reachable =
            ArtifactReachability::build(&roots, &[], &leases, ReachabilityLimits::default())?;
        let collector = ArtifactGarbageCollector::new(
            &owner.root,
            ArtifactCleanupLimits::publication_retirement(),
        );
        let now_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let report = collector.run_cycle(&reachable, now_unix_ns)?;
        self.automatic_retirement_requested
            .store(false, Ordering::Release);
        Ok(Some(report))
    }

    fn validate_filesystem_owner(&self, expected: ControlRecord) -> FormatResult<&Path> {
        let owner = self
            .filesystem
            .as_ref()
            .ok_or(FormatError::InvalidFilesystemOwner {
                detail: "publisher has no filesystem owner",
            })?;
        if owner.database_id != expected.database_id() {
            return Err(FormatError::InvalidFilesystemOwner {
                detail: "publisher database identity differs from runtime CONTROL",
            });
        }
        let writer_lock = owner.writer_lock.lock();
        let writer_lock = writer_lock
            .as_ref()
            .ok_or(FormatError::InvalidFilesystemOwner {
                detail: "publisher filesystem authority has been revoked",
            })?;
        validate_lock_root(writer_lock, &owner.root)?;
        validate_control_owner(&owner.root, expected)?;
        Ok(&owner.root)
    }

    fn validate_external_root(&self, candidate: &Path) -> FormatResult<()> {
        let owner = self
            .filesystem
            .as_ref()
            .ok_or(FormatError::InvalidFilesystemOwner {
                detail: "publisher has no filesystem owner",
            })?;
        let writer_lock = owner.writer_lock.lock();
        let writer_lock = writer_lock
            .as_ref()
            .ok_or(FormatError::InvalidFilesystemOwner {
                detail: "publisher filesystem authority has been revoked",
            })?;
        match writer_lock.validate_root(candidate) {
            Ok(true) => Ok(()),
            Ok(false) => Err(FormatError::InvalidFilesystemOwner {
                detail: "filesystem collaborator belongs to another database root",
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(FormatError::InvalidFilesystemOwner {
                    detail: "filesystem collaborator root or LOCK file no longer exists",
                })
            }
            Err(error) => Err(FormatError::FilesystemOwnerIo {
                operation: "validate collaborator root",
                kind: error.kind(),
            }),
        }
    }

    fn validate_staging_path(&self, candidate: &Path) -> FormatResult<()> {
        let owner = self
            .filesystem
            .as_ref()
            .ok_or(FormatError::InvalidFilesystemOwner {
                detail: "publisher has no filesystem owner",
            })?;
        let canonical_candidate =
            fs::canonicalize(candidate).map_err(|error| FormatError::FilesystemOwnerIo {
                operation: "resolve staged publication",
                kind: error.kind(),
            })?;
        let canonical_staging = fs::canonicalize(owner.root.join("staging")).map_err(|error| {
            FormatError::FilesystemOwnerIo {
                operation: "resolve owned staging root",
                kind: error.kind(),
            }
        })?;
        if canonical_candidate == canonical_staging
            || !canonical_candidate.starts_with(&canonical_staging)
        {
            return Err(FormatError::InvalidFilesystemOwner {
                detail: "staged publication belongs to another database root",
            });
        }
        Ok(())
    }
}

fn validate_lock_root(writer_lock: &FileLock, root: &Path) -> FormatResult<()> {
    match writer_lock.validate_root(root) {
        Ok(true) => Ok(()),
        Ok(false) => Err(FormatError::InvalidFilesystemOwner {
            detail: "database root or LOCK file was moved or replaced",
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(FormatError::InvalidFilesystemOwner {
                detail: "database root or LOCK file was moved or replaced",
            })
        }
        Err(error) => Err(FormatError::FilesystemOwnerIo {
            operation: "validate owned root",
            kind: error.kind(),
        }),
    }
}

fn validate_control_owner(root: &Path, expected: ControlRecord) -> FormatResult<()> {
    let name = match expected.slot() {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    };
    let path = root.join(name);
    let metadata = fs::symlink_metadata(&path).map_err(|error| FormatError::FilesystemOwnerIo {
        operation: "inspect owned CONTROL",
        kind: error.kind(),
    })?;
    if !metadata.file_type().is_file() || metadata.len() != CONTROL_RECORD_BYTES as u64 {
        return Err(FormatError::InvalidFilesystemOwner {
            detail: "owned CONTROL is not one exact regular slot",
        });
    }
    let bytes = fs::read(path).map_err(|error| FormatError::FilesystemOwnerIo {
        operation: "read owned CONTROL",
        kind: error.kind(),
    })?;
    let actual = decode_control_slot(&bytes, expected.slot())?;
    if actual != expected {
        return Err(FormatError::InvalidFilesystemOwner {
            detail: "owned CONTROL differs from runtime publication source",
        });
    }
    Ok(())
}

fn publish_runtime(
    current: &ArcSwap<PhysicalGenerationSnapshot>,
    snapshot: &Arc<PhysicalGenerationSnapshot>,
) -> FormatResult<()> {
    reach_generation_boundary(GenerationCrashPoint::RuntimeGenerationBeforePublish).map_err(
        |error| FormatError::PublicationRecoveryRequired {
            operation: "inject before runtime generation publication",
            kind: error.kind(),
        },
    )?;
    current.store(Arc::clone(snapshot));
    reach_generation_boundary(GenerationCrashPoint::RuntimeGenerationPublished).map_err(|error| {
        FormatError::PublicationRecoveryRequired {
            operation: "inject after runtime generation publication",
            kind: error.kind(),
        }
    })
}

pub trait IndexReplacementPublicationSink {
    /// Canonical or aliased path to the database root mutated by this sink.
    fn database_root(&self) -> &Path;

    /// Atomically move the already file-synced staged `.idx` to its final
    /// locator and sync the final parent directory before returning success.
    fn publish_staged_index_and_sync(
        &mut self,
        replacement: &StagedIndexReplacement,
    ) -> FormatResult<()>;

    /// Publish the immutable table manifest and sync its parent directory.
    fn publish_table_manifest_and_sync(
        &mut self,
        reference: TableManifestRef,
        bytes: &[u8],
    ) -> FormatResult<()>;

    /// Publish the immutable database manifest and sync its parent directory.
    fn publish_database_manifest_and_sync(
        &mut self,
        reference: DatabaseManifestRootRef,
        bytes: &[u8],
    ) -> FormatResult<()>;

    /// Replace and sync the inactive CONTROL slot. An implementation must not
    /// return an error after the new slot may already be durable; that
    /// indeterminate outcome belongs to the recovery-required state machine in
    /// CA-60.
    fn publish_control_and_sync(
        &mut self,
        slot: ControlSlotIndex,
        bytes: &[u8; CONTROL_RECORD_BYTES],
    ) -> FormatResult<()>;
}

#[derive(Debug)]
pub struct PreparedIndexReplacement {
    expected_control: ControlRecord,
    staged: StagedIndexReplacement,
    superseded_index: Option<ArtifactRef>,
    table_reference: TableManifestRef,
    table_manifest_bytes: Vec<u8>,
    database_reference: DatabaseManifestRootRef,
    database_manifest_bytes: Vec<u8>,
    control: ControlRecord,
    control_bytes: [u8; CONTROL_RECORD_BYTES],
    snapshot: PhysicalGenerationSnapshot,
}

impl PreparedIndexReplacement {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        current: &PhysicalGenerationLease,
        staged: StagedIndexReplacement,
        table_manifest_id: ManifestId,
        database_manifest_id: ManifestId,
        writer_instance_id: WriterInstanceId,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        let snapshot = current.snapshot();
        let control = snapshot.control();
        let database_manifest = snapshot.database_manifest();
        let written = staged.written();
        let rebuild = written.rebuild();
        if rebuild.database_id() != control.database_id() {
            return Err(invalid("index rebuild targets another database"));
        }
        let source_table = snapshot
            .table_manifest(rebuild.table_id())
            .ok_or_else(|| invalid("index rebuild table is absent from current generation"))?;
        let source_segment = snapshot
            .segment(rebuild.table_id(), rebuild.segment_id())
            .ok_or_else(|| invalid("index rebuild segment is absent from current generation"))?;
        if source_segment.data_artifact() != rebuild.data_artifact()
            || written.source_data() != rebuild.data_artifact()
        {
            return Err(invalid(
                "index rebuild source data differs from current segment",
            ));
        }
        if written.catalog_generation() != database_manifest.catalog().generation() {
            return Err(invalid(
                "replacement index catalog generation differs from current catalog",
            ));
        }
        let target_generation = control.database_generation().checked_next()?;
        if written.reference().creation_generation() != target_generation {
            return Err(invalid(
                "replacement index creation generation is not the next database generation",
            ));
        }
        if snapshot
            .artifact_references()
            .iter()
            .any(|reference| reference.id() == written.reference().id())
        {
            return Err(invalid("replacement index reuses an existing artifact ID"));
        }
        if database_manifest.manifest_id() == database_manifest_id
            || database_manifest
                .tables()
                .iter()
                .any(|entry| entry.manifest().id() == table_manifest_id)
        {
            return Err(invalid("replacement generation reuses a manifest ID"));
        }

        let segments = source_table
            .segments()
            .iter()
            .copied()
            .map(|segment| {
                if segment.id() != rebuild.segment_id() {
                    return Ok(segment);
                }
                SegmentDescriptor::new_at_tier(
                    segment.id(),
                    segment.kind(),
                    segment.tier(),
                    segment.min_transaction_id(),
                    segment.max_transaction_id(),
                    segment.row_count(),
                    segment.first_row_id(),
                    segment.last_row_id(),
                    segment.data_artifact(),
                    Some(written.reference()),
                )
            })
            .collect::<FormatResult<Vec<_>>>()?;
        let table_manifest = TableManifest::new(
            source_table.database_id(),
            source_table.table_id(),
            table_manifest_id,
            ManifestGeneration::new(target_generation.get())?,
            written.catalog_generation(),
            source_table.row_id_high_water(),
            source_table.next_segment_sequence(),
            segments,
            created_unix_ns,
        )?;
        let table_manifest_bytes = encode_table_manifest(&table_manifest)?;
        let table_reference = TableManifestRef::new(
            source_table.table_id(),
            ManifestRef::new(
                table_manifest_id,
                ManifestKind::Table,
                table_manifest.generation(),
                table_manifest_bytes.len() as u64,
                footer_sha(&table_manifest_bytes),
            )?,
        )?;

        let table_references = database_manifest
            .tables()
            .iter()
            .copied()
            .map(|reference| {
                if reference.table_id() == source_table.table_id() {
                    table_reference
                } else {
                    reference
                }
            })
            .collect();
        let successor_database_manifest = DatabaseManifest::new(
            database_manifest.database_id(),
            database_manifest_id,
            target_generation,
            database_manifest.catalog(),
            database_manifest.wal_replay_floor(),
            database_manifest.transaction_high_water(),
            table_references,
            created_unix_ns,
        )?;
        let database_manifest_bytes = encode_database_manifest(&successor_database_manifest)?;
        let database_reference = DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(target_generation.get())?,
            footer_sha(&database_manifest_bytes),
        );
        let successor_control = ControlRecord::new(
            inactive_slot(control.slot()),
            target_generation,
            control.database_id(),
            database_reference,
            control.catalog(),
            control.wal_replay_floor(),
            created_unix_ns,
            writer_instance_id,
        )?;
        let control_bytes = encode_control_slot(successor_control);
        let table_manifests = snapshot
            .table_manifests()
            .iter()
            .cloned()
            .map(|manifest| {
                if manifest.table_id() == table_manifest.table_id() {
                    table_manifest.clone()
                } else {
                    manifest
                }
            })
            .collect();
        let successor_snapshot = PhysicalGenerationSnapshot::new(
            successor_control,
            successor_database_manifest,
            table_manifests,
        )?;

        Ok(Self {
            expected_control: control,
            staged,
            superseded_index: source_segment.index_artifact(),
            table_reference,
            table_manifest_bytes,
            database_reference,
            database_manifest_bytes,
            control: successor_control,
            control_bytes,
            snapshot: successor_snapshot,
        })
    }

    pub const fn staged(&self) -> &StagedIndexReplacement {
        &self.staged
    }

    pub const fn superseded_index(&self) -> Option<ArtifactRef> {
        self.superseded_index
    }

    pub const fn table_reference(&self) -> TableManifestRef {
        self.table_reference
    }

    pub const fn database_reference(&self) -> DatabaseManifestRootRef {
        self.database_reference
    }

    pub const fn control(&self) -> ControlRecord {
        self.control
    }
}

fn validate_root(control: ControlRecord, manifest: &DatabaseManifest) -> FormatResult<()> {
    if manifest.database_id() != control.database_id()
        || manifest.manifest_id() != control.database_manifest().id()
        || manifest.generation() != control.database_generation()
        || manifest.generation().get() != control.database_manifest().generation().get()
        || manifest.catalog().id() != control.catalog().id()
        || manifest.catalog().generation() != control.catalog().generation()
        || manifest.catalog().body_sha256() != control.catalog().body_sha256()
        || manifest.wal_replay_floor() != control.wal_replay_floor()
    {
        return Err(invalid(
            "runtime database manifest differs from CONTROL root",
        ));
    }
    Ok(())
}

const fn inactive_slot(slot: ControlSlotIndex) -> ControlSlotIndex {
    match slot {
        ControlSlotIndex::Zero => ControlSlotIndex::One,
        ControlSlotIndex::One => ControlSlotIndex::Zero,
    }
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..]
        .try_into()
        .expect("encoded manifest has a complete common footer")
}

const fn invalid(detail: &'static str) -> FormatError {
    FormatError::InvalidManifest {
        kind: "index replacement generation",
        detail,
    }
}
