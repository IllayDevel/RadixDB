use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use radixdb_catalog::{CatalogGeneration as RuntimeCatalogGeneration, CatalogPackMeta, ObjectKind};

use crate::v6::publication::filesystem::source::GenerationFileSource;
use crate::v6::{
    decode_catalog_wal, select_control_slots, validate_control_generation_with_limits,
    CatalogRecoveryReport, ControlRecord, ControlSlotIndex, DataWalRecoveryContext, FormatError,
    FormatResult, PhysicalGenerationSnapshot, RecoveredDatabase, RecoveryLimits, UnavailableIndex,
    ValidatedGeneration, WalRecovery, CONTROL_RECORD_BYTES,
};

pub struct DatabaseRecovery {
    root: PathBuf,
    limits: RecoveryLimits,
}

impl DatabaseRecovery {
    pub fn new(root: impl AsRef<Path>, limits: RecoveryLimits) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            limits,
        }
    }

    pub fn recover<Wal>(&self, wal: &mut Wal) -> FormatResult<RecoveredDatabase<Wal::State>>
    where
        Wal: WalRecovery,
    {
        let control_0 = read_control_slot(&self.root, ControlSlotIndex::Zero)?;
        let control_1 = read_control_slot(&self.root, ControlSlotIndex::One)?;
        let mut prepared = None;
        let mut newest_candidate_error = None;
        let mut resource_error = None;
        let selected = select_control_slots(&control_0, &control_1, |candidate| {
            if resource_error.is_some() {
                return false;
            }
            match self.prepare_candidate(*candidate, wal) {
                Ok(candidate) => {
                    prepared = Some(candidate);
                    true
                }
                Err(error @ FormatError::MetadataOpenLimitExceeded { .. }) => {
                    resource_error = Some(error);
                    false
                }
                Err(error) => {
                    // Candidate traversal is newest-first. Preserve the first
                    // concrete graph/replay failure so an entirely unusable
                    // root reports its violated invariant instead of erasing
                    // it behind the generic no-complete-generation outcome.
                    newest_candidate_error.get_or_insert(error);
                    false
                }
            }
        });
        if let Some(error) = resource_error {
            return Err(error);
        }
        let selected = match selected {
            Ok(selected) => selected,
            Err(FormatError::NoCompleteControlGeneration) => {
                return Err(
                    newest_candidate_error.unwrap_or(FormatError::NoCompleteControlGeneration)
                );
            }
            Err(error) => return Err(error),
        };
        let prepared = prepared.ok_or(FormatError::NoCompleteControlGeneration)?;
        if prepared.control != selected {
            return Err(invalid("selected CONTROL differs from prepared candidate"));
        }

        let data_outcome = wal.replay_data(DataWalRecoveryContext::new(
            &self.root,
            selected.wal_replay_floor(),
            &prepared.physical,
            &prepared.catalog,
            prepared.metadata_allowance,
        ))?;
        let (runtime_state, data_report, runtime_table_ids) = data_outcome.into_parts();
        validate_data_replay(
            selected,
            &prepared.catalog,
            data_report.last_lsn(),
            &runtime_table_ids,
        )?;

        Ok(RecoveredDatabase::new(
            self.root.clone(),
            selected,
            prepared.physical,
            prepared.catalog,
            prepared.unavailable_indexes,
            prepared.catalog_report,
            data_report,
            runtime_state,
        ))
    }

    fn prepare_candidate(
        &self,
        control: ControlRecord,
        wal: &mut impl WalRecovery,
    ) -> FormatResult<PreparedRecovery> {
        let validated =
            validate_database_root_files(&self.root, control, self.limits.reachability())?;
        prepare_catalog_replay(control, validated, wal, self.limits)
    }
}

pub(crate) fn validate_database_root_files(
    root: &Path,
    control: ControlRecord,
    limits: crate::v6::ReachabilityLimits,
) -> FormatResult<ValidatedGeneration> {
    let mut source = GenerationFileSource::for_recovery(root);
    validate_control_generation_with_limits(control, &mut source, limits).map_err(|error| {
        match error {
            crate::v6::ReachabilityError::ReachabilityLimitExceeded {
                field,
                actual,
                limit,
            } => FormatError::MetadataOpenLimitExceeded {
                field,
                actual,
                limit,
            },
            error => FormatError::InvalidRecoveryGraph {
                detail: error.to_string(),
            },
        }
    })
}

struct PreparedRecovery {
    control: ControlRecord,
    physical: PhysicalGenerationSnapshot,
    catalog: RuntimeCatalogGeneration,
    unavailable_indexes: Vec<UnavailableIndex>,
    catalog_report: CatalogRecoveryReport,
    metadata_allowance: crate::v6::ReachabilityAllowance,
}

fn prepare_catalog_replay(
    control: ControlRecord,
    validated: ValidatedGeneration,
    wal: &mut impl WalRecovery,
    limits: RecoveryLimits,
) -> FormatResult<PreparedRecovery> {
    let (database_manifest, table_manifests, catalog_pack, unavailable_indexes, metadata_allowance) =
        validated.into_runtime_parts();
    let floor = control.wal_replay_floor();
    let wal_bytes =
        wal.read_catalog_transactions(floor, limits.catalog_wal().max_stream_bytes())?;
    if wal_bytes.len() as u64 > limits.catalog_wal().max_stream_bytes() {
        return Err(FormatError::CatalogWalLimitExceeded {
            field: "stream bytes",
            actual: wal_bytes.len() as u64,
            limit: limits.catalog_wal().max_stream_bytes(),
        });
    }
    let replay = decode_catalog_wal(&wal_bytes, limits.catalog_wal())?;
    let base = RuntimeCatalogGeneration::from_pack(catalog_pack);
    let base_meta = base.meta();
    let catalog =
        crate::v6::catalog_wal::replay_catalog_wal_after_owned(base, &replay, floor.lsn())?;
    validate_catalog_replay(control, base_meta, &catalog)?;
    let replayed_transactions = replay
        .transactions()
        .iter()
        .filter(|transaction| transaction.commit_lsn() > floor.lsn())
        .count() as u64;
    let catalog_report = CatalogRecoveryReport::new(
        floor,
        replay.transactions().len() as u64,
        replayed_transactions,
        replay.committed_bytes() as u64,
        replay.incomplete_tail_bytes() as u64,
        crate::v6::CatalogGeneration::new(base_meta.catalog_generation())?,
        crate::v6::CatalogGeneration::new(catalog.meta().catalog_generation())?,
    );
    let physical = PhysicalGenerationSnapshot::new(control, database_manifest, table_manifests)?;
    Ok(PreparedRecovery {
        control,
        physical,
        catalog,
        unavailable_indexes,
        catalog_report,
        metadata_allowance,
    })
}

fn validate_catalog_replay(
    control: ControlRecord,
    base_meta: CatalogPackMeta,
    catalog: &RuntimeCatalogGeneration,
) -> FormatResult<()> {
    let final_meta = catalog.meta();
    if base_meta.database_id() != control.database_id().into_bytes()
        || final_meta.database_id() != base_meta.database_id()
        || base_meta.catalog_id() != control.catalog().id().into_bytes()
        || base_meta.catalog_generation() != control.catalog().generation().get()
        || final_meta.catalog_generation() < base_meta.catalog_generation()
        || final_meta.snapshot_lsn() < base_meta.snapshot_lsn()
    {
        return Err(invalid(
            "catalog WAL result differs from selected CONTROL/base catalog",
        ));
    }
    Ok(())
}

fn validate_data_replay(
    control: ControlRecord,
    catalog: &RuntimeCatalogGeneration,
    last_lsn: u64,
    runtime_table_ids: &[radixdb_catalog::ObjectId],
) -> FormatResult<()> {
    if last_lsn < control.wal_replay_floor().lsn() {
        return Err(invalid("data WAL replay stopped before the selected floor"));
    }
    if runtime_table_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            "data WAL runtime table identities are not strictly ordered",
        ));
    }
    let mut catalog_tables = catalog
        .objects_of_kind(ObjectKind::Table)
        .map(|table| table.id())
        .collect::<Vec<_>>();
    catalog_tables.sort_unstable();
    if catalog_tables != runtime_table_ids {
        return Err(invalid(
            "data WAL runtime table set differs from recovered catalog",
        ));
    }
    Ok(())
}

fn read_control_slot(root: &Path, slot: ControlSlotIndex) -> FormatResult<Vec<u8>> {
    let name = match slot {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    };
    let path = root.join(name);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(recovery_io("inspect CONTROL slot", error)),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != CONTROL_RECORD_BYTES as u64
    {
        return Ok(Vec::new());
    }
    let mut file = open_regular(&path).map_err(|error| recovery_io("open CONTROL slot", error))?;
    let mut bytes = vec![0_u8; CONTROL_RECORD_BYTES];
    file.read_exact(&mut bytes)
        .map_err(|error| recovery_io("read CONTROL slot", error))?;
    Ok(bytes)
}

fn open_regular(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options.open(path)
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

const fn invalid(detail: &'static str) -> FormatError {
    FormatError::InvalidRecovery { detail }
}

fn recovery_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::RecoveryIo {
        operation,
        kind: error.kind(),
    }
}
