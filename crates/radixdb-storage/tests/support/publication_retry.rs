use super::*;

#[cfg(target_os = "linux")]
#[path = "crashable_filesystem.rs"]
mod crashable;
#[path = "publication_sync_trace.rs"]
mod trace;
use trace::{SyncEvent, Trace};

fn members(target: &GenerationFiles) -> Vec<PathBuf> {
    vec![
        target.data_reference.relative_path(),
        target.index_reference.relative_path(),
        table_manifest_path(target.table_reference),
        catalog_path(target.catalog_reference),
        wal_path(target.wal_floor),
        database_manifest_path(target.database_reference),
    ]
}

fn publish(
    fixture: &Fixture,
    build: &ArtifactBuildLease,
    staged: &CompleteStagingSet,
) -> FormatResult<()> {
    fixture
        .publisher
        .publish_staged_generation(build.clone(), staged.clone(), fixture.target.control)
        .map(|_| ())
}

fn assert_not_published(fixture: &Fixture, old_control: &[u8]) {
    assert!(!fixture.root.path().join("CONTROL.1").exists());
    assert_eq!(
        std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap(),
        old_control
    );
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
}

fn assert_synced_before_control(events: &[SyncEvent], directory: &Path, root: &Path) {
    let control = events
        .iter()
        .position(|event| event.path == root.join("CONTROL.1") && event.succeeded)
        .expect("CONTROL fsync missing from syscall trace");
    assert!(
        events[..control]
            .iter()
            .any(|event| event.path == directory && event.succeeded),
        "directory {} was not durably synced before CONTROL: {events:?}",
        directory.display()
    );
}

fn assert_complete(fixture: &Fixture, events: &[SyncEvent]) {
    let root = fixture.root.path();
    for member in members(&fixture.target) {
        let final_path = root.join(&member);
        assert!(final_path.is_file());
        // Include every ancestor edge: an existing name may come from a failed mkdir sync.
        let mut directory = final_path.parent().unwrap();
        loop {
            assert_synced_before_control(events, directory, root);
            if directory == root {
                break;
            }
            directory = directory.parent().unwrap();
        }
    }
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        8
    );
    let recovered = DatabaseRecovery::new(root, RecoveryLimits::default())
        .recover(&mut ProcessRecoveryWal)
        .unwrap();
    assert_eq!(recovered.control(), fixture.target.control);
}

#[test]
fn syscall_oracle_observes_real_sync_and_isolates_injected_failure() {
    let root = tempfile::tempdir().unwrap();
    let directory = std::fs::File::open(root.path()).unwrap();
    let trace = Trace::start(Some(root.path()), false, None);
    assert_eq!(
        directory.sync_all().unwrap_err().raw_os_error(),
        Some(libc::EIO)
    );
    directory.sync_all().unwrap();
    let (events, _, _) = trace.finish();
    assert_eq!(events.len(), 2);
    assert!(!events[0].succeeded);
    assert!(events[1].succeeded);
    assert_eq!(events[1].path, root.path());
}

#[test]
fn retry_syncs_preexisting_member_and_directory_names_before_control() {
    for existing in ["moved", "copied", "linked"] {
        let fixture = fixture();
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);
        for member in members(&fixture.target) {
            let source = staged.path().join(&member);
            let target = fixture.root.path().join(&member);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            match existing {
                "moved" => std::fs::rename(source, target).unwrap(),
                "copied" => {
                    std::fs::copy(source, target).unwrap();
                }
                "linked" => std::fs::hard_link(source, target).unwrap(),
                _ => unreachable!(),
            }
        }
        let trace = Trace::start(None, false, None);
        publish(&fixture, &build, &staged).unwrap();
        let (events, _, _) = trace.finish();
        assert_complete(&fixture, &events);
    }
}

#[test]
fn retry_after_each_member_rename_restores_directory_durability() {
    for point in [
        GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::IndexAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::TableManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::CatalogPackAfterRenameBeforeDirSync,
        GenerationCrashPoint::DatabaseManifestAfterRenameBeforeDirSync,
    ] {
        let fixture = fixture();
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);
        let old_control = std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap();
        let fault = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(publish(&fixture, &build, &staged).is_err());
        assert_eq!(fault.hit_count(), 1);
        drop(fault);
        assert_not_published(&fixture, &old_control);
        let trace = Trace::start(None, false, None);
        publish(&fixture, &build, &staged).unwrap();
        let (events, _, _) = trace.finish();
        assert_complete(&fixture, &events);
    }
}

#[test]
fn sync_failure_on_existing_directory_or_member_stops_control_and_is_retryable() {
    for target in ["ancestor", "final", "staged"] {
        let fixture = fixture();
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);
        let member = fixture.target.data_reference.relative_path();
        let final_file = fixture.root.path().join(&member);
        let staged_file = staged.path().join(&member);
        let fail = match target {
            "ancestor" => final_file.parent().unwrap().parent().unwrap().to_path_buf(),
            "final" => final_file.parent().unwrap().to_path_buf(),
            "staged" => staged_file.parent().unwrap().to_path_buf(),
            _ => unreachable!(),
        };
        let old_control = std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap();
        let trace = Trace::start(Some(&fail), false, None);
        assert!(
            publish(&fixture, &build, &staged).is_err(),
            "injection must be reached: {target}"
        );
        let (events, _, _) = trace.finish();
        assert!(events
            .iter()
            .any(|event| event.path == fail && !event.succeeded));
        assert_not_published(&fixture, &old_control);
        // Repeat the SAME sync failure after the name already exists.
        let trace = Trace::start(Some(&fail), false, None);
        assert!(
            publish(&fixture, &build, &staged).is_err(),
            "retry bypasses {target} sync"
        );
        let (events, _, _) = trace.finish();
        assert!(events
            .iter()
            .any(|event| event.path == fail && !event.succeeded));
        assert_not_published(&fixture, &old_control);
        let trace = Trace::start(None, false, None);
        publish(&fixture, &build, &staged).unwrap();
        let (events, _, _) = trace.finish();
        assert_synced_before_control(&events, &fail, fixture.root.path());
        assert_complete(&fixture, &events);
    }
}

#[test]
fn link_fallback_and_interrupted_unlink_preserve_retry_durability() {
    for interrupt in [false, true] {
        let fixture = fixture();
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);
        let source = staged
            .path()
            .join(fixture.target.data_reference.relative_path());
        let final_path = fixture
            .root
            .path()
            .join(fixture.target.data_reference.relative_path());
        let trace = Trace::start(None, true, interrupt.then_some(source.as_path()));
        let result = publish(&fixture, &build, &staged);
        let (events, fallbacks, unlink_failures) = trace.finish();
        assert!(fallbacks > 0);
        if interrupt {
            assert!(result.is_err());
            assert_eq!(unlink_failures, 1);
            assert!(source.is_file() && final_path.is_file());
            assert!(!fixture.root.path().join("CONTROL.1").exists());
            let trace = Trace::start(None, true, None);
            publish(&fixture, &build, &staged).unwrap();
            let (events, _, _) = trace.finish();
            assert_complete(&fixture, &events);
        } else {
            result.unwrap();
            assert_eq!(unlink_failures, 0);
            assert_complete(&fixture, &events);
        }
    }
}

#[test]
fn every_existing_member_sync_error_stops_control_until_successful_retry() {
    for member_index in 0..6 {
        for both_exist in [false, true] {
            let fixture = fixture();
            let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
            let build = begin_seal(&fixture.publisher, &staged);
            let member = &members(&fixture.target)[member_index];
            let source = staged.path().join(member);
            let target = fixture.root.path().join(member);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            if both_exist {
                std::fs::copy(&source, &target).unwrap();
            } else {
                std::fs::rename(&source, &target).unwrap();
            }
            let before = std::fs::read(&target).unwrap();
            let old_control = std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap();
            let trace = Trace::start(Some(&target), false, None);
            assert!(publish(&fixture, &build, &staged).is_err());
            let (events, _, _) = trace.finish();
            assert!(events
                .iter()
                .any(|event| event.path == target && !event.succeeded));
            assert_not_published(&fixture, &old_control);
            assert_eq!(std::fs::read(&target).unwrap(), before);
            let trace = Trace::start(None, false, None);
            publish(&fixture, &build, &staged).unwrap();
            let (events, _, _) = trace.finish();
            assert_synced_before_control(&events, &target, fixture.root.path());
            assert_complete(&fixture, &events);
        }
    }
}

#[test]
fn retry_with_removed_empty_staging_directories_syncs_surviving_parent() {
    let fixture = fixture();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);
    for member in members(&fixture.target) {
        let source = staged.path().join(&member);
        let target = fixture.root.path().join(&member);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::rename(source, target).unwrap();
    }
    for directory in ["artifacts", "manifests", "catalog", "wal"] {
        // Only this test's now-empty member hierarchy is removed; markers remain.
        std::fs::remove_dir_all(staged.path().join(directory)).unwrap();
    }
    let trace = Trace::start(None, false, None);
    publish(&fixture, &build, &staged).unwrap();
    let (events, _, _) = trace.finish();
    assert_synced_before_control(&events, staged.path(), fixture.root.path());
    assert_complete(&fixture, &events);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "isolated fuse2fs storage-crash evidence; requires e2fsprogs and FUSE"]
fn crashable_filesystem_selects_complete_old_or_retried_generation() {
    for retry_before_crash in [false, true] {
        let mut filesystem = crashable::CrashableFilesystem::new();
        let fixture = fixture_with_group_rows_in(filesystem.mount_path(), 2);
        filesystem.sync_baseline(fixture.root.path());
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);

        let fault = GenerationFaultGuard::arm(
            GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync,
            GenerationFaultMode::ReturnIoError,
        );
        assert!(publish(&fixture, &build, &staged).is_err());
        assert_eq!(fault.hit_count(), 1);
        drop(fault);

        let expected_generation = if retry_before_crash {
            publish(&fixture, &build, &staged).unwrap();
            8
        } else {
            7
        };
        let database_root = fixture.root.path().to_path_buf();
        // The mount process is about to disappear abruptly. Keep test-owned
        // paths from attempting cleanup against the disconnected mount.
        std::mem::forget(fixture);

        filesystem.crash_repair_and_remount();
        let recovered = DatabaseRecovery::new(&database_root, RecoveryLimits::default())
            .recover(&mut ProcessRecoveryWal)
            .unwrap();
        assert_eq!(
            recovered.control().database_generation().get(),
            expected_generation
        );
        assert!(!recovered.physical().artifact_references().is_empty());
        for artifact in recovered.physical().artifact_references() {
            assert!(database_root.join(artifact.relative_path()).is_file());
        }
        filesystem.shutdown();
    }
}

#[test]
#[ignore = "release-only publication latency evidence with explicit output and TMPDIR"]
fn publication_sync_cost_evidence() {
    let output = std::env::var_os("REVIEW_SYNC_EVIDENCE").expect("REVIEW_SYNC_EVIDENCE required");
    let temporary = std::env::var_os("TMPDIR").expect("explicit TMPDIR required");
    let mut results = Vec::new();
    for resumed in [false, true] {
        let mut samples = Vec::new();
        let mut sync_counts = Vec::new();
        for iteration in 0..11 {
            let fixture = fixture();
            let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
            let build = begin_seal(&fixture.publisher, &staged);
            if resumed {
                for member in members(&fixture.target) {
                    let target = fixture.root.path().join(&member);
                    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
                    std::fs::rename(staged.path().join(member), target).unwrap();
                }
            }
            let trace = Trace::start(None, false, None);
            let start = std::time::Instant::now();
            publish(&fixture, &build, &staged).unwrap();
            let elapsed = start.elapsed().as_nanos() as u64;
            let (events, _, _) = trace.finish();
            if iteration > 0 {
                samples.push(elapsed);
                sync_counts.push(events.len());
            }
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        results.push(serde_json::json!({
            "resumed": resumed, "members": 6, "samples_ns": samples,
            "median_ns": (sorted[4] + sorted[5]) / 2, "fsync_calls": sync_counts,
        }));
    }
    let evidence = serde_json::json!({"tmpdir": temporary.to_string_lossy(), "results": results});
    std::fs::write(output, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    eprintln!("{evidence}");
}
