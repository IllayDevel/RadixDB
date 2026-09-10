use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use sha2::{Digest, Sha256};

use crate::v6::{
    catalog_path, commit_snapshot_manifest, database_manifest_path, open_snapshot_manifest,
    restore_snapshot, table_manifest_path, ArtifactKind, ReachabilityLimits, SnapshotId,
    SnapshotIndexPolicy, SnapshotManifest, SnapshotMember, SnapshotMemberKind, WriterInstanceId,
    SNAPSHOT_MANIFEST_FILE,
};
use crate::PhysicalSnapshotIdentity;

use super::*;

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_SNAPSHOT_DIRECTORIES: usize = 4_096;
const RESTORE_STATE_FILE: &str = ".physical-restore-state";
const RESTORE_STAGING_DIRECTORY: &str = ".physical-restore-staging";
const RESTORE_BACKUP_DIRECTORY: &str = ".physical-restore-backup";
const RESTORE_STATE_MAGIC: &[u8; 4] = b"RDXR";
const RESTORE_STATE_VERSION: u32 = 1;
const RESTORE_COMPONENTS: [&str; 8] = [
    "CONTROL.0",
    "CONTROL.1",
    "wal",
    "catalog",
    "manifests",
    "artifacts",
    "staging",
    "quarantine",
];

type MemberIdentity = (SnapshotMemberKind, [u8; 16]);

impl MVCCEngine {
    pub(super) fn create_physical_snapshot(&self) -> Result<PhysicalSnapshotIdentity> {
        self.create_physical_snapshot_cancellable(&|| false)
    }

    pub(super) fn create_physical_snapshot_cancellable(
        &self,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<PhysicalSnapshotIdentity> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }
        if self.path == "memory://" {
            return Err(Error::invalid_argument(
                "SNAPSHOT requires a persistent database",
            ));
        }
        let persistence = self
            .persistence()
            .filter(|manager| manager.is_enabled())
            .ok_or_else(|| {
                Error::invalid_argument("SNAPSHOT requires an enabled persistent database")
            })?;

        let snapshot_root = persistence.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_root).map_err(|error| {
            Error::internal(format!(
                "failed to create physical snapshot root '{}': {error}",
                snapshot_root.display()
            ))
        })?;

        while self
            .compaction_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            check_snapshot_cancelled(is_cancelled)?;
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let compaction_guard = AtomicBoolGuard(&self.compaction_running);

        let ddl_guard = loop {
            check_snapshot_cancelled(is_cancelled)?;
            if let Some(guard) = DdlFenceGuard::try_shared(Arc::clone(&self.ddl_fence)) {
                break guard;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let checkpoint_guard = self.lock_checkpoint_mutex_cancellable(is_cancelled)?;

        // Freeze one commit boundary in the same lock order as checkpoint:
        // DDL -> checkpoint -> global seal.  A transaction holds the shared
        // seal side from final certification through WAL marker and runtime
        // publication, so the exclusive side waits for every in-flight commit
        // and prevents a new one from entering while CONTROL and the required
        // WAL suffix are pinned together.
        let seal_guard = loop {
            check_snapshot_cancelled(is_cancelled)?;
            if let Some(guard) = self
                .seal_fence
                .try_write_for(std::time::Duration::from_millis(10))
            {
                break guard;
            }
        };

        let publisher = self.physical_generation.load_full().ok_or_else(|| {
            Error::internal("physical generation publisher is unavailable during snapshot")
        })?;
        let source = publisher.pin().map_err(snapshot_format_error)?;
        let generation = source.snapshot();
        let wal_sources = persistence
            .freeze_snapshot_generations(generation.database_manifest().wal_replay_floor())?;
        drop(seal_guard);

        let snapshot_id = SnapshotId::new();
        let snapshot_directory = snapshot_root.join(snapshot_id.to_string());
        std::fs::create_dir(&snapshot_directory).map_err(|error| {
            Error::internal(format!(
                "failed to create physical snapshot directory '{}': {error}",
                snapshot_directory.display()
            ))
        })?;
        let mut cleanup = IncompleteSnapshot::new(snapshot_directory.clone());

        let mut wal_members = Vec::with_capacity(wal_sources.len());
        for wal in &wal_sources {
            check_snapshot_cancelled(is_cancelled)?;
            let locator = SnapshotMember::wal(
                generation.control().database_id(),
                wal.generation(),
                wal.byte_length(),
                [0; 32],
            )
            .map_err(snapshot_format_error)?;
            let digest = copy_snapshot_member(
                wal.path(),
                &snapshot_directory.join(locator.relative_path()),
                wal.byte_length(),
                is_cancelled,
            )?;
            wal_members.push(
                SnapshotMember::wal(
                    generation.control().database_id(),
                    wal.generation(),
                    wal.byte_length(),
                    digest,
                )
                .map_err(snapshot_format_error)?,
            );
        }

        let database_manifest = generation.control().database_manifest();
        let database_manifest_source =
            Path::new(&self.path).join(database_manifest_path(database_manifest));
        let database_manifest_bytes = regular_file_length(&database_manifest_source)?;
        let manifest = SnapshotManifest::from_generation(
            snapshot_id,
            generation,
            database_manifest_bytes,
            wal_members,
            SnapshotIndexPolicy::Include,
            snapshot_unix_nanos(),
        )
        .map_err(snapshot_format_error)?;
        let sources = physical_member_sources(Path::new(&self.path), generation, &manifest)?;

        for member in manifest
            .members()
            .iter()
            .copied()
            .filter(|member| member.kind() != SnapshotMemberKind::Wal)
        {
            check_snapshot_cancelled(is_cancelled)?;
            let source = sources
                .get(&(member.kind(), member.id()))
                .ok_or_else(|| Error::internal("snapshot member has no physical source"))?;
            copy_snapshot_member(
                source,
                &snapshot_directory.join(member.relative_path()),
                member.byte_length(),
                is_cancelled,
            )?;
        }

        if let Err(error) = commit_snapshot_manifest(&snapshot_directory, &manifest) {
            if snapshot_directory.join(SNAPSHOT_MANIFEST_FILE).is_file() {
                cleanup.disarm();
            }
            return Err(snapshot_format_error(error));
        }
        cleanup.disarm();

        // The immutable generation and WAL suffix are no longer needed after
        // the manifest is durable. Retention deliberately runs outside every
        // engine maintenance fence.
        drop(source);
        drop(checkpoint_guard);
        drop(ddl_guard);
        drop(compaction_guard);
        if let Err(error) = retain_latest_snapshots(&snapshot_root, persistence.keep_count()) {
            eprintln!(
                "Warning: physical snapshot {snapshot_id} committed, but retention failed: {error}"
            );
        }
        Ok(PhysicalSnapshotIdentity {
            snapshot_id: snapshot_id.to_string(),
            database_id: manifest.database_id().to_string(),
            physical_format_major: crate::v6::FORMAT_VERSION.major(),
            physical_format_minor: crate::v6::FORMAT_VERSION.minor(),
        })
    }

    pub(super) fn restore_physical_snapshot(&self, requested: Option<&str>) -> Result<String> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }
        if self.path == "memory://" {
            return Err(Error::invalid_argument(
                "RESTORE requires a persistent database",
            ));
        }
        let persistence = self
            .persistence()
            .filter(|manager| manager.is_enabled())
            .ok_or_else(|| Error::invalid_argument("RESTORE requires enabled persistence"))?;

        while self
            .compaction_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let compaction_guard = AtomicBoolGuard(&self.compaction_running);
        let ddl_guard = DdlFenceGuard::exclusive(Arc::clone(&self.ddl_fence));
        let checkpoint_guard = self.lock_checkpoint_mutex_profiled();

        self.registry.stop_accepting_transactions();
        let mut admission = RestoreAdmission::new(&self.registry);
        let remaining = self
            .registry
            .wait_for_active_transactions(std::time::Duration::from_secs(5));
        if remaining != 0 {
            return Err(Error::internal(format!(
                "cannot restore while {remaining} transactions remain active"
            )));
        }

        let database_root = Path::new(&self.path);
        let snapshot_root = persistence.path().join("snapshots");
        let selected = select_snapshot(&snapshot_root, requested)?;
        let selected_manifest = open_snapshot_manifest(&selected).map_err(snapshot_format_error)?;
        let staging = database_root.join(RESTORE_STAGING_DIRECTORY);
        prepare_restore_staging(database_root, selected_manifest.snapshot_id())?;
        let snapshot_id = selected_manifest.snapshot_id();
        match restore_snapshot(
            &selected,
            &staging,
            WriterInstanceId::new(),
            ReachabilityLimits::default(),
        )
        .map_err(snapshot_format_error)
        {
            Ok(outcome) => {
                if outcome.snapshot_id() != snapshot_id || outcome.target_root() != staging {
                    return Err(Error::internal(
                        "physical restore outcome differs from the selected snapshot",
                    ));
                }
            }
            Err(error) if staging.exists() => {
                // The absent-root restore publishes its complete target before
                // syncing the parent directory. If that final sync reports an
                // indeterminate outcome, the target itself is authoritative
                // evidence: validate it again and make the parent durable here.
                validate_restore_staging(&staging)?;
                sync_snapshot_directory(database_root)?;
                eprintln!(
                    "Warning: physical restore staging publication required local recovery: {error}"
                );
            }
            Err(error) => return Err(error),
        }

        if let Err(error) = self.swap_physical_restore(&staging) {
            if self.persistence().is_none() {
                admission.disarm();
                self.open.store(false, Ordering::Release);
                *self.lifecycle.write().unwrap() = EngineLifecycleState::Failed(error.clone());
            }
            return Err(error);
        }

        admission.resume();
        drop(checkpoint_guard);
        drop(ddl_guard);
        drop(compaction_guard);
        Ok(snapshot_id.to_string())
    }

    fn swap_physical_restore(&self, staging: &Path) -> Result<()> {
        validate_restore_staging(staging)?;
        let database_root = Path::new(&self.path);
        let backup = database_root.join(RESTORE_BACKUP_DIRECTORY);
        prepare_restore_backup(database_root)?;
        std::fs::create_dir(&backup).map_err(|error| {
            Error::internal(format!(
                "failed to create physical restore backup '{}': {error}",
                backup.display()
            ))
        })?;

        let state = PhysicalRestoreState {
            phase: PhysicalRestorePhase::Prepared,
            old_components: existing_component_mask(database_root)?,
        };
        write_physical_restore_state(database_root, state)?;

        let old_persistence = self.persistence().ok_or_else(|| {
            Error::internal("physical restore requires an active persistence manager")
        })?;
        let mut rollback_state_is_durable = true;
        let result = (|| -> Result<()> {
            old_persistence.stop()?;
            self.persistence.store(None);
            self.physical_generation.store(None);

            move_restore_components(database_root, &backup, state.old_components)?;
            sync_snapshot_directory(database_root)?;
            write_physical_restore_state(
                database_root,
                PhysicalRestoreState {
                    phase: PhysicalRestorePhase::OldMoved,
                    ..state
                },
            )?;
            #[cfg(any(test, feature = "test-failpoints"))]
            if crate::test_failpoints::RESTORE_FAIL_AFTER_OLD_MOVE.load(Ordering::Acquire) {
                return Err(Error::internal(
                    "failpoint: physical restore after old move",
                ));
            }

            install_restore_components(staging, database_root)?;
            sync_snapshot_directory(database_root)?;
            write_physical_restore_state(
                database_root,
                PhysicalRestoreState {
                    phase: PhysicalRestorePhase::NewMoved,
                    ..state
                },
            )?;
            #[cfg(any(test, feature = "test-failpoints"))]
            if crate::test_failpoints::RESTORE_FAIL_AFTER_NEW_MOVE.load(Ordering::Acquire) {
                return Err(Error::internal(
                    "failpoint: physical restore after new move",
                ));
            }

            self.reload_physical_generation_after_restore()?;
            if let Err(commit_error) = write_physical_restore_state(
                database_root,
                PhysicalRestoreState {
                    phase: PhysicalRestorePhase::Committed,
                    ..state
                },
            ) {
                if let Err(repair_error) = write_physical_restore_state(
                    database_root,
                    PhysicalRestoreState {
                        phase: PhysicalRestorePhase::NewMoved,
                        ..state
                    },
                ) {
                    rollback_state_is_durable = false;
                    return Err(Error::internal(format!(
                        "restore commit failed ({commit_error}) and rollback-state repair failed ({repair_error})"
                    )));
                }
                return Err(Error::internal(format!(
                    "physical restore commit publication failed: {commit_error}"
                )));
            }
            Ok(())
        })();

        if let Err(error) = result {
            if let Some(manager) = self.persistence() {
                let _ = manager.stop();
            }
            self.persistence.store(None);
            self.physical_generation.store(None);
            self.clear_runtime_for_physical_restore();
            if !rollback_state_is_durable {
                return Err(error);
            }
            return match reconcile_physical_restore(database_root)
                .and_then(|()| self.reload_physical_generation_after_restore())
            {
                Ok(()) => Err(Error::internal(format!(
                    "physical restore failed and the previous generation was restored: {error}"
                ))),
                Err(rollback_error) => Err(Error::internal(format!(
                    "physical restore failed ({error}) and rollback failed ({rollback_error})"
                ))),
            };
        }

        if let Err(error) = cleanup_committed_restore(database_root) {
            // Committed is the durable authority. Retaining its state marker
            // makes cleanup retryable on the next open; it must not turn a
            // completed restore into an ambiguous API failure.
            eprintln!("Warning: physical restore committed, but cleanup requires retry: {error}");
        }
        Ok(())
    }

    fn reload_physical_generation_after_restore(&self) -> Result<()> {
        self.clear_runtime_for_physical_restore();
        self.registry.reset_after_failed_startup();
        let persistence_config = self.config.read().unwrap().persistence.clone();
        let writer_lock = self.file_lock.lock().unwrap().clone().ok_or_else(|| {
            Error::internal("physical restore reload requires the database writer lock")
        })?;
        match self.recover_persistent_runtime(&persistence_config, &writer_lock) {
            Ok(publisher) => {
                self.physical_generation.store(Some(publisher));
                Ok(())
            }
            Err(error) => {
                if let Some(manager) = self.persistence() {
                    let _ = manager.stop();
                }
                self.persistence.store(None);
                self.physical_generation.store(None);
                self.clear_runtime_for_physical_restore();
                Err(error)
            }
        }
    }

    fn clear_runtime_for_physical_restore(&self) {
        {
            let mut stores = self.version_stores.write().unwrap();
            for store in stores.values() {
                store.close();
            }
            stores.clear();
        }
        self.schemas.write().unwrap().clear();
        self.pending_tables.write().unwrap().clear();
        self.txn_version_stores.write().unwrap().clear();
        self.views.write().unwrap().clear();
        self.segment_managers.write().unwrap().clear();
        self.schema_epoch.store(0, Ordering::Release);
        *self.fk_reverse_cache.write().unwrap() = (u64::MAX, StringMap::default());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum PhysicalRestorePhase {
    Prepared = 1,
    OldMoved = 2,
    NewMoved = 3,
    Committed = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PhysicalRestoreState {
    phase: PhysicalRestorePhase,
    old_components: u16,
}

impl PhysicalRestoreState {
    const ENCODED_BYTES: usize = 16;

    fn encode(self) -> [u8; Self::ENCODED_BYTES] {
        let mut bytes = [0_u8; Self::ENCODED_BYTES];
        bytes[..4].copy_from_slice(RESTORE_STATE_MAGIC);
        bytes[4..8].copy_from_slice(&RESTORE_STATE_VERSION.to_le_bytes());
        bytes[8] = self.phase as u8;
        bytes[10..12].copy_from_slice(&self.old_components.to_le_bytes());
        let checksum = crc32fast::hash(&bytes[..12]);
        bytes[12..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::ENCODED_BYTES || &bytes[..4] != RESTORE_STATE_MAGIC {
            return Err(Error::internal("invalid physical restore state header"));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().expect("bounded version field"));
        if version != RESTORE_STATE_VERSION {
            return Err(Error::internal(format!(
                "unsupported physical restore state version {version}"
            )));
        }
        if bytes[9] != 0 {
            return Err(Error::internal(
                "physical restore state has non-zero reserved flags",
            ));
        }
        let stored = u32::from_le_bytes(bytes[12..16].try_into().expect("bounded checksum field"));
        if stored != crc32fast::hash(&bytes[..12]) {
            return Err(Error::internal("physical restore state checksum mismatch"));
        }
        let phase = match bytes[8] {
            1 => PhysicalRestorePhase::Prepared,
            2 => PhysicalRestorePhase::OldMoved,
            3 => PhysicalRestorePhase::NewMoved,
            4 => PhysicalRestorePhase::Committed,
            value => {
                return Err(Error::internal(format!(
                    "invalid physical restore phase {value}"
                )))
            }
        };
        let old_components = u16::from_le_bytes(
            bytes[10..12]
                .try_into()
                .expect("bounded component-mask field"),
        );
        let allowed = (1_u16 << RESTORE_COMPONENTS.len()) - 1;
        if old_components & !allowed != 0 {
            return Err(Error::internal(
                "physical restore state has an invalid component mask",
            ));
        }
        if old_components & 0b11 == 0 {
            return Err(Error::internal(
                "physical restore state does not preserve a CONTROL slot",
            ));
        }
        Ok(Self {
            phase,
            old_components,
        })
    }
}

struct RestoreAdmission<'a> {
    registry: &'a TransactionRegistry,
    resume_on_drop: bool,
}

impl<'a> RestoreAdmission<'a> {
    const fn new(registry: &'a TransactionRegistry) -> Self {
        Self {
            registry,
            resume_on_drop: true,
        }
    }

    fn resume(&mut self) {
        if self.resume_on_drop {
            self.registry.start_accepting_transactions();
            self.resume_on_drop = false;
        }
    }

    fn disarm(&mut self) {
        self.resume_on_drop = false;
    }
}

impl Drop for RestoreAdmission<'_> {
    fn drop(&mut self) {
        if self.resume_on_drop {
            self.registry.start_accepting_transactions();
        }
    }
}

fn select_snapshot(snapshot_root: &Path, requested: Option<&str>) -> Result<PathBuf> {
    require_restore_directory(snapshot_root, "snapshot root")?;
    if let Some(requested) = requested {
        let id = SnapshotId::from_str(requested).map_err(|error| {
            Error::invalid_argument(format!(
                "invalid physical snapshot ID '{requested}': {error}"
            ))
        })?;
        let path = snapshot_root.join(id.to_string());
        require_restore_directory(&path, "requested snapshot")?;
        let manifest = open_snapshot_manifest(&path).map_err(snapshot_format_error)?;
        if manifest.snapshot_id() != id {
            return Err(Error::internal(
                "requested snapshot directory identity differs from SNAPSHOT.mft",
            ));
        }
        return Ok(path);
    }

    let mut selected = None;
    for (index, entry) in std::fs::read_dir(snapshot_root)
        .map_err(|error| {
            Error::internal(format!(
                "failed to enumerate snapshot root '{}': {error}",
                snapshot_root.display()
            ))
        })?
        .enumerate()
    {
        if index >= MAX_SNAPSHOT_DIRECTORIES {
            return Err(Error::internal(format!(
                "snapshot root exceeds the bounded directory limit {MAX_SNAPSHOT_DIRECTORIES}"
            )));
        }
        let entry = entry
            .map_err(|error| Error::internal(format!("failed to read snapshot entry: {error}")))?;
        let metadata = std::fs::symlink_metadata(entry.path()).map_err(|error| {
            Error::internal(format!(
                "failed to inspect snapshot entry '{}': {error}",
                entry.path().display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let Ok(id) = entry
            .file_name()
            .to_str()
            .ok_or(())
            .and_then(|name| SnapshotId::from_str(name).map_err(|_| ()))
        else {
            continue;
        };
        let Ok(manifest) = open_snapshot_manifest(entry.path()) else {
            continue;
        };
        if manifest.snapshot_id() != id {
            continue;
        }
        let key = (manifest.created_unix_ns(), id.into_bytes());
        if selected
            .as_ref()
            .is_none_or(|(current, _): &((u64, [u8; 16]), PathBuf)| key > *current)
        {
            selected = Some((key, entry.path()));
        }
    }
    selected
        .map(|(_, path)| path)
        .ok_or_else(|| Error::invalid_argument("no committed physical snapshot found"))
}

fn prepare_restore_staging(database_root: &Path, snapshot_id: SnapshotId) -> Result<()> {
    if database_root.join(RESTORE_STATE_FILE).exists() {
        return Err(Error::internal(
            "physical restore state must be reconciled before starting another restore",
        ));
    }
    remove_restore_path_if_present(&database_root.join(RESTORE_STAGING_DIRECTORY))?;
    remove_restore_path_if_present(&database_root.join(format!(".restore-{snapshot_id}")))?;
    remove_restore_path_if_present(&database_root.join(format!("{RESTORE_STATE_FILE}.tmp")))?;
    sync_snapshot_directory(database_root)
}

fn prepare_restore_backup(database_root: &Path) -> Result<()> {
    let backup = database_root.join(RESTORE_BACKUP_DIRECTORY);
    let metadata = match std::fs::symlink_metadata(&backup) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Error::internal(format!(
                "failed to inspect physical restore backup '{}': {error}",
                backup.display()
            )))
        }
        Ok(metadata) => metadata,
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::internal(
            "physical restore backup is not a real directory",
        ));
    }
    if std::fs::read_dir(&backup)
        .map_err(|error| {
            Error::internal(format!(
                "failed to inspect physical restore backup '{}': {error}",
                backup.display()
            ))
        })?
        .next()
        .is_some()
    {
        return Err(Error::internal(
            "non-empty physical restore backup exists without durable state",
        ));
    }
    std::fs::remove_dir(&backup).map_err(|error| {
        Error::internal(format!(
            "failed to retire empty physical restore backup '{}': {error}",
            backup.display()
        ))
    })?;
    sync_snapshot_directory(database_root)
}

fn validate_restore_staging(staging: &Path) -> Result<()> {
    require_restore_directory(staging, "physical restore staging root")?;
    let mut present = 0_u16;
    for (entry_index, entry) in std::fs::read_dir(staging)
        .map_err(|error| {
            Error::internal(format!(
                "failed to enumerate physical restore staging '{}': {error}",
                staging.display()
            ))
        })?
        .enumerate()
    {
        if entry_index >= RESTORE_COMPONENTS.len() {
            return Err(Error::internal(
                "physical restore staging contains too many root components",
            ));
        }
        let entry = entry.map_err(|error| {
            Error::internal(format!(
                "failed to read physical restore component: {error}"
            ))
        })?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            Error::internal("physical restore staging contains a non-UTF-8 component")
        })?;
        let index = RESTORE_COMPONENTS
            .iter()
            .position(|component| *component == name)
            .ok_or_else(|| {
                Error::internal(format!(
                    "physical restore staging contains unknown component '{name}'"
                ))
            })?;
        validate_restore_component(&entry.path(), name)?;
        present |= 1_u16 << index;
    }
    let required = component_bit("CONTROL.0")
        | component_bit("wal")
        | component_bit("catalog")
        | component_bit("manifests");
    if present & required != required {
        return Err(Error::internal(
            "physical restore staging is missing a required root component",
        ));
    }
    Ok(())
}

fn require_restore_directory(path: &Path, role: &str) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        Error::internal(format!(
            "failed to inspect {role} '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::internal(format!(
            "{role} '{}' is not a real directory",
            path.display()
        )));
    }
    Ok(())
}

fn validate_restore_component(path: &Path, name: &str) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        Error::internal(format!(
            "failed to inspect physical restore component '{name}': {error}"
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(Error::internal(format!(
            "physical restore component '{name}' is a symlink"
        )));
    }
    let valid = if name.starts_with("CONTROL.") {
        metadata.is_file()
    } else {
        metadata.is_dir()
    };
    if !valid {
        return Err(Error::internal(format!(
            "physical restore component '{name}' has the wrong filesystem type"
        )));
    }
    Ok(())
}

fn component_bit(name: &str) -> u16 {
    let index = RESTORE_COMPONENTS
        .iter()
        .position(|component| *component == name)
        .expect("restore component is statically declared");
    1_u16 << index
}

fn existing_component_mask(database_root: &Path) -> Result<u16> {
    let mut mask = 0_u16;
    for (index, component) in RESTORE_COMPONENTS.iter().enumerate() {
        let path = database_root.join(component);
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::internal(format!(
                    "failed to inspect live component '{component}': {error}"
                )))
            }
            Ok(_) => {
                validate_restore_component(&path, component)?;
                mask |= 1_u16 << index;
            }
        }
    }
    if mask & 0b11 == 0 {
        return Err(Error::internal(
            "live physical generation has no CONTROL slot",
        ));
    }
    Ok(mask)
}

fn write_physical_restore_state(database_root: &Path, state: PhysicalRestoreState) -> Result<()> {
    write_snapshot_artifact_atomic(&database_root.join(RESTORE_STATE_FILE), &state.encode())
}

fn move_restore_components(database_root: &Path, backup: &Path, component_mask: u16) -> Result<()> {
    for (index, component) in RESTORE_COMPONENTS.iter().enumerate() {
        if component_mask & (1_u16 << index) == 0 {
            continue;
        }
        let source = database_root.join(component);
        validate_restore_component(&source, component)?;
        let target = backup.join(component);
        require_absent_restore_path(&target)?;
        std::fs::rename(&source, &target).map_err(|error| {
            Error::internal(format!(
                "failed to preserve live component '{component}': {error}"
            ))
        })?;
    }
    sync_snapshot_directory(backup)?;
    sync_snapshot_directory(database_root)
}

fn install_restore_components(staging: &Path, database_root: &Path) -> Result<()> {
    validate_restore_staging(staging)?;
    for component in RESTORE_COMPONENTS {
        let source = staging.join(component);
        match std::fs::symlink_metadata(&source) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(Error::internal(format!(
                    "failed to inspect staged component '{component}': {error}"
                )))
            }
            Ok(_) => validate_restore_component(&source, component)?,
        }
        let target = database_root.join(component);
        require_absent_restore_path(&target)?;
        std::fs::rename(&source, &target).map_err(|error| {
            Error::internal(format!(
                "failed to install restored component '{component}': {error}"
            ))
        })?;
    }
    sync_snapshot_directory(staging)?;
    sync_snapshot_directory(database_root)
}

fn require_absent_restore_path(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::internal(format!(
            "failed to inspect restore path '{}': {error}",
            path.display()
        ))),
        Ok(_) => Err(Error::internal(format!(
            "restore path '{}' already exists",
            path.display()
        ))),
    }
}

fn remove_restore_path_if_present(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Error::internal(format!(
                "failed to inspect restore artifact '{}': {error}",
                path.display()
            )))
        }
        Ok(metadata) => metadata,
    };
    let result = if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    result.map_err(|error| {
        Error::internal(format!(
            "failed to remove restore artifact '{}': {error}",
            path.display()
        ))
    })
}

fn cleanup_committed_restore(database_root: &Path) -> Result<()> {
    remove_restore_path_if_present(&database_root.join(RESTORE_STAGING_DIRECTORY))?;
    remove_restore_path_if_present(&database_root.join(RESTORE_BACKUP_DIRECTORY))?;
    remove_restore_path_if_present(&database_root.join(format!("{RESTORE_STATE_FILE}.tmp")))?;
    sync_snapshot_directory(database_root)?;
    remove_restore_path_if_present(&database_root.join(RESTORE_STATE_FILE))?;
    sync_snapshot_directory(database_root)
}

pub(super) fn reconcile_physical_restore(database_root: &Path) -> Result<()> {
    let state_path = database_root.join(RESTORE_STATE_FILE);
    if !state_path.exists() {
        return cleanup_orphan_restore_artifacts(database_root);
    }
    let state = PhysicalRestoreState::decode(&std::fs::read(&state_path).map_err(|error| {
        Error::internal(format!(
            "failed to read physical restore state '{}': {error}",
            state_path.display()
        ))
    })?)?;
    if state.phase == PhysicalRestorePhase::Committed {
        return cleanup_committed_restore(database_root);
    }

    let backup = database_root.join(RESTORE_BACKUP_DIRECTORY);
    for (index, component) in RESTORE_COMPONENTS.iter().enumerate() {
        let live = database_root.join(component);
        let old = backup.join(component);
        let had_old = state.old_components & (1_u16 << index) != 0;
        if old.exists() {
            remove_restore_path_if_present(&live)?;
            std::fs::rename(&old, &live).map_err(|error| {
                Error::internal(format!(
                    "failed to restore previous component '{component}': {error}"
                ))
            })?;
        } else if had_old && state.phase != PhysicalRestorePhase::Prepared {
            return Err(Error::internal(format!(
                "restore rollback is missing previous component '{component}'"
            )));
        } else if !had_old {
            remove_restore_path_if_present(&live)?;
        }
    }
    if backup.exists() {
        sync_snapshot_directory(&backup)?;
    }
    remove_restore_path_if_present(&database_root.join(RESTORE_STAGING_DIRECTORY))?;
    remove_restore_path_if_present(&backup)?;
    std::fs::remove_file(&state_path).map_err(|error| {
        Error::internal(format!(
            "failed to clear physical restore state '{}': {error}",
            state_path.display()
        ))
    })?;
    sync_snapshot_directory(database_root)
}

fn cleanup_orphan_restore_artifacts(database_root: &Path) -> Result<()> {
    let backup = database_root.join(RESTORE_BACKUP_DIRECTORY);
    if backup.exists() {
        prepare_restore_backup(database_root)?;
    }
    remove_restore_path_if_present(&database_root.join(RESTORE_STAGING_DIRECTORY))?;
    remove_restore_path_if_present(&database_root.join(format!("{RESTORE_STATE_FILE}.tmp")))?;

    let mut orphan_staging = Vec::new();
    for (index, entry) in std::fs::read_dir(database_root)
        .map_err(|error| {
            Error::internal(format!(
                "failed to enumerate database root during restore reconciliation: {error}"
            ))
        })?
        .enumerate()
    {
        if index >= MAX_SNAPSHOT_DIRECTORIES {
            return Err(Error::internal(
                "database root exceeds restore reconciliation entry limit",
            ));
        }
        let entry = entry.map_err(|error| {
            Error::internal(format!("failed to read database root entry: {error}"))
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(raw_id) = name.strip_prefix(".restore-") else {
            continue;
        };
        if SnapshotId::from_str(raw_id).is_ok() {
            orphan_staging.push(entry.path());
        }
    }
    for path in orphan_staging {
        remove_restore_path_if_present(&path)?;
    }
    sync_snapshot_directory(database_root)
}

fn physical_member_sources(
    database_root: &Path,
    generation: &crate::v6::PhysicalGenerationSnapshot,
    manifest: &SnapshotManifest,
) -> Result<BTreeMap<MemberIdentity, PathBuf>> {
    let mut sources = BTreeMap::new();
    let database_reference = generation.control().database_manifest();
    insert_source(
        &mut sources,
        SnapshotMemberKind::DatabaseManifest,
        database_reference.id().into_bytes(),
        database_root.join(database_manifest_path(database_reference)),
    )?;
    let catalog_reference = generation.database_manifest().catalog();
    insert_source(
        &mut sources,
        SnapshotMemberKind::Catalog,
        catalog_reference.id().into_bytes(),
        database_root.join(catalog_path(catalog_reference)),
    )?;
    for reference in generation.database_manifest().tables().iter().copied() {
        insert_source(
            &mut sources,
            SnapshotMemberKind::TableManifest,
            reference.manifest().id().into_bytes(),
            database_root.join(table_manifest_path(reference)),
        )?;
    }
    for reference in generation.artifact_references() {
        let kind = match reference.kind() {
            ArtifactKind::Data => SnapshotMemberKind::Data,
            ArtifactKind::Index => SnapshotMemberKind::Index,
        };
        insert_source(
            &mut sources,
            kind,
            reference.id().into_bytes(),
            database_root.join(reference.relative_path()),
        )?;
    }

    let expected = manifest
        .members()
        .iter()
        .filter(|member| member.kind() != SnapshotMemberKind::Wal)
        .count();
    if sources.len() != expected {
        return Err(Error::internal(format!(
            "physical snapshot source count {} differs from manifest count {expected}",
            sources.len()
        )));
    }
    Ok(sources)
}

fn insert_source(
    sources: &mut BTreeMap<MemberIdentity, PathBuf>,
    kind: SnapshotMemberKind,
    id: [u8; 16],
    path: PathBuf,
) -> Result<()> {
    if sources.insert((kind, id), path).is_some() {
        return Err(Error::internal(
            "physical snapshot generation repeats a member identity",
        ));
    }
    Ok(())
}

fn copy_snapshot_member(
    source: &Path,
    target: &Path,
    expected_bytes: u64,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<[u8; 32]> {
    let actual_bytes = regular_file_length(source)?;
    if actual_bytes != expected_bytes {
        return Err(Error::internal(format!(
            "snapshot source '{}' is {actual_bytes} bytes; expected {expected_bytes}",
            source.display()
        )));
    }
    let parent = target
        .parent()
        .ok_or_else(|| Error::internal("snapshot member has no parent directory"))?;
    std::fs::create_dir_all(parent).map_err(|error| {
        Error::internal(format!(
            "failed to create snapshot member directory '{}': {error}",
            parent.display()
        ))
    })?;

    let mut source_options = OpenOptions::new();
    source_options.read(true);
    set_no_follow(&mut source_options);
    let mut input = source_options.open(source).map_err(|error| {
        Error::internal(format!(
            "failed to open snapshot source '{}': {error}",
            source.display()
        ))
    })?;
    let mut target_options = OpenOptions::new();
    target_options.write(true).create_new(true);
    set_no_follow(&mut target_options);
    let mut output = target_options.open(target).map_err(|error| {
        Error::internal(format!(
            "failed to create snapshot member '{}': {error}",
            target.display()
        ))
    })?;

    let mut digest = Sha256::new();
    let mut remaining = expected_bytes;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    while remaining != 0 {
        check_snapshot_cancelled(is_cancelled)?;
        let length = usize::try_from(remaining.min(COPY_BUFFER_BYTES as u64))
            .expect("bounded snapshot copy length fits usize");
        input.read_exact(&mut buffer[..length]).map_err(|error| {
            Error::internal(format!(
                "failed to read snapshot source '{}': {error}",
                source.display()
            ))
        })?;
        output.write_all(&buffer[..length]).map_err(|error| {
            Error::internal(format!(
                "failed to write snapshot member '{}': {error}",
                target.display()
            ))
        })?;
        digest.update(&buffer[..length]);
        remaining -= length as u64;
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing).map_err(|error| {
        Error::internal(format!(
            "failed to verify snapshot source '{}': {error}",
            source.display()
        ))
    })? != 0
    {
        return Err(Error::internal(format!(
            "snapshot source '{}' grew while being copied",
            source.display()
        )));
    }
    output.flush().map_err(|error| {
        Error::internal(format!(
            "failed to flush snapshot member '{}': {error}",
            target.display()
        ))
    })?;
    Ok(digest.finalize().into())
}

fn regular_file_length(path: &Path) -> Result<u64> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        Error::internal(format!(
            "failed to inspect snapshot source '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::internal(format!(
            "snapshot source '{}' is not a regular file",
            path.display()
        )));
    }
    Ok(metadata.len())
}

fn retain_latest_snapshots(snapshot_root: &Path, keep_count: usize) -> Result<()> {
    let mut committed = Vec::new();
    for entry in std::fs::read_dir(snapshot_root).map_err(|error| {
        Error::internal(format!(
            "failed to enumerate snapshot root '{}': {error}",
            snapshot_root.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            Error::internal(format!("failed to read snapshot directory entry: {error}"))
        })?;
        let file_type = entry.file_type().map_err(|error| {
            Error::internal(format!(
                "failed to inspect snapshot directory entry: {error}"
            ))
        })?;
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        if let Ok(manifest) = open_snapshot_manifest(entry.path()) {
            committed.push((
                manifest.created_unix_ns(),
                manifest.snapshot_id(),
                entry.path(),
            ));
        }
    }
    committed.sort_unstable_by_key(|(created, id, _)| (*created, id.into_bytes()));
    let remove_count = committed.len().saturating_sub(keep_count.max(1));
    for (_, _, path) in committed.into_iter().take(remove_count) {
        std::fs::remove_dir_all(&path).map_err(|error| {
            Error::internal(format!(
                "failed to retire snapshot '{}': {error}",
                path.display()
            ))
        })?;
    }
    sync_snapshot_directory(snapshot_root)
}

fn check_snapshot_cancelled(is_cancelled: &(dyn Fn() -> bool + Send + Sync)) -> Result<()> {
    if is_cancelled() {
        Err(Error::QueryCancelled)
    } else {
        Ok(())
    }
}

fn snapshot_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn snapshot_format_error(error: crate::v6::FormatError) -> Error {
    Error::internal(format!("physical snapshot failed: {error}"))
}

fn sync_snapshot_directory(path: &Path) -> Result<()> {
    #[cfg(not(windows))]
    {
        let directory = std::fs::File::open(path).map_err(|error| {
            Error::internal(format!(
                "failed to open snapshot directory '{}' for sync: {error}",
                path.display()
            ))
        })?;
        directory.sync_all().map_err(|error| {
            Error::internal(format!(
                "failed to sync snapshot directory '{}': {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn write_snapshot_artifact_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::internal("snapshot artifact has no parent directory"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::internal("snapshot artifact filename is not UTF-8"))?;
    let temporary = parent.join(format!("{file_name}.tmp"));
    let result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&temporary).map_err(|error| {
            Error::internal(format!(
                "failed to create snapshot artifact '{}': {error}",
                temporary.display()
            ))
        })?;
        file.write_all(bytes).map_err(|error| {
            Error::internal(format!(
                "failed to write snapshot artifact '{}': {error}",
                temporary.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            Error::internal(format!(
                "failed to sync snapshot artifact '{}': {error}",
                temporary.display()
            ))
        })?;
        std::fs::rename(&temporary, path).map_err(|error| {
            Error::internal(format!(
                "failed to publish snapshot artifact '{}': {error}",
                path.display()
            ))
        })?;
        sync_snapshot_directory(parent)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

struct IncompleteSnapshot {
    path: PathBuf,
    armed: bool,
}

impl IncompleteSnapshot {
    const fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for IncompleteSnapshot {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}
