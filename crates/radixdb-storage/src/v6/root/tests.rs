use std::path::Path;

use super::{DatabaseRoot, DatabaseRootState};
use crate::v6::{
    DataWalRecoveryContext, DataWalRecoveryOutcome, DatabaseRecovery, FormatError, FormatResult,
    GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode, RecoveryLimits, WalRecovery,
    WalReplayFloor,
};

struct EmptyWal;

impl WalRecovery for EmptyWal {
    type State = ();

    fn read_catalog_transactions(
        &mut self,
        _floor: WalReplayFloor,
        _byte_budget: u64,
    ) -> FormatResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>> {
        Ok(DataWalRecoveryOutcome::new(
            (),
            context.floor().lsn(),
            0,
            0,
            0,
            vec![],
        ))
    }
}

#[test]
fn fresh_root_publishes_one_recoverable_empty_generation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("database");

    let state = DatabaseRoot::new(&root).open_or_create(100).unwrap();
    let control = match state {
        DatabaseRootState::Created(control) => control,
        other => panic!("unexpected root state: {other:?}"),
    };

    assert_eq!(control.database_generation().get(), 1);
    assert_eq!(control.catalog().generation().get(), 1);
    assert_eq!(control.wal_replay_floor().generation().get(), 1);
    assert_eq!(control.wal_replay_floor().lsn(), 0);
    assert!(root.join("CONTROL.0").is_file());
    assert!(!root.join("CONTROL.1").exists());
    assert!(root.join("wal/wal-0000000000000001.log").is_file());
    assert!(root.join("catalog/catalog-0000000000000001.cat").is_file());
    assert!(root
        .join("manifests/database-0000000000000001.mft")
        .is_file());
    assert!(!contains_suffix(&root, ".vol"));
    assert!(!contains_suffix(&root, ".rpi"));

    let recovered = DatabaseRecovery::new(&root, RecoveryLimits::default())
        .recover(&mut EmptyWal)
        .unwrap();
    assert_eq!(recovered.control(), control);
    assert_eq!(recovered.catalog().graph().objects().len(), 1);
    assert!(matches!(
        DatabaseRoot::new(&root).open_or_create(200).unwrap(),
        DatabaseRootState::Existing
    ));
}

#[test]
fn legacy_root_is_rejected_with_the_migration_error() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("volumes")).unwrap();

    let error = DatabaseRoot::new(directory.path())
        .open_or_create(100)
        .unwrap_err();

    assert_eq!(error, FormatError::LegacyDatabaseRoot);
    let message = error.to_string();
    assert!(message.contains("frozen old binary"));
    assert!(message.contains("import the SQL dump"));
    assert!(!directory.path().join("CONTROL.0").exists());
}

#[test]
fn initial_publication_resumes_after_an_immutable_member_move() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("database");
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::CatalogPackAfterRenameBeforeDirSync,
        GenerationFaultMode::ReturnIoError,
    );

    let error = DatabaseRoot::new(&root).open_or_create(100).unwrap_err();
    assert!(matches!(error, FormatError::PublicationIo { .. }));
    assert_eq!(guard.hit_count(), 1);
    drop(guard);
    assert!(!root.join("CONTROL.0").exists());
    assert!(root.join("catalog/catalog-0000000000000001.cat").is_file());

    let state = DatabaseRoot::new(&root).open_or_create(200).unwrap();
    assert!(matches!(state, DatabaseRootState::Resumed(_)));
    let recovered = DatabaseRecovery::new(&root, RecoveryLimits::default())
        .recover(&mut EmptyWal)
        .unwrap();
    assert_eq!(recovered.control().database_generation().get(), 1);
    assert_eq!(recovered.catalog().graph().objects().len(), 1);
}

#[test]
fn final_members_without_control_and_staging_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("catalog")).unwrap();
    std::fs::write(directory.path().join("catalog/orphan.cat"), b"orphan").unwrap();

    let error = DatabaseRoot::new(directory.path())
        .open_or_create(100)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidDatabaseRoot { .. }));
    assert_eq!(
        std::fs::read(directory.path().join("catalog/orphan.cat")).unwrap(),
        b"orphan"
    );
}

fn contains_suffix(root: &Path, suffix: &str) -> bool {
    std::fs::read_dir(root).unwrap().flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            contains_suffix(&path, suffix)
        } else {
            path.to_string_lossy().ends_with(suffix)
        }
    })
}
