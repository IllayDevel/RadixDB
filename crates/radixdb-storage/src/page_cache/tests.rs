use super::*;
use crate::mvcc::MVCCEngine;
use crate::{Config, Engine};
use radixdb_core::{DataType, Row, SchemaBuilder, Value};

fn create_catalog_fixture_table(
    engine: &MVCCEngine,
    schema: radixdb_core::Schema,
) -> radixdb_core::Result<()> {
    // Page-cache tests exercise the persistent production tree through the
    // storage engine's catalog-aware unit-test fixture.
    crate::mvcc::engine::tests::support::create_catalog_test_table(engine, schema).map(|_| ())
}

fn create_persistent_fixture(path: &Path, rows: std::ops::RangeInclusive<i64>) -> MVCCEngine {
    let mut config = Config::with_path(path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.checkpoint_on_close = false;
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    create_catalog_fixture_table(
        &engine,
        SchemaBuilder::new("warm_items")
            .column("id", DataType::Integer, false, true)
            .column("payload", DataType::Text, false, false)
            .build(),
    )
    .unwrap();
    for id in rows {
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table("warm_items").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(id),
                Value::text(format!("warmup-payload-{id:04}")),
            ]))
            .unwrap();
        drop(table);
        transaction.commit().unwrap();
    }
    engine.force_checkpoint_cycle().unwrap();
    engine
}

fn inventory_bytes(inventory: &WarmupInventory) -> Vec<(PathBuf, Vec<u8>)> {
    inventory
        .files
        .iter()
        .map(|file| (file.path.clone(), std::fs::read(&file.path).unwrap()))
        .collect()
}

#[test]
fn target_is_bounded_by_level_memory_and_explicit_cap() {
    let level = calculate_target(
        10_000,
        WarmupPolicy {
            level: 5,
            max_bytes: 0,
            memory_reserve: 1_000,
        },
        Some(20_000),
    );
    assert_eq!(level.target, 5_000);
    assert_eq!(level.limited_by, "page_cache_level");

    let memory = calculate_target(
        10_000,
        WarmupPolicy {
            level: 10,
            max_bytes: 0,
            memory_reserve: 3_000,
        },
        Some(7_000),
    );
    assert_eq!(memory.target, 4_000);
    assert_eq!(memory.limited_by, "available_memory");

    let cap = calculate_target(
        10_000,
        WarmupPolicy {
            level: 10,
            max_bytes: 2_000,
            memory_reserve: 1_000,
        },
        Some(20_000),
    );
    assert_eq!(cap.target, 2_000);
    assert_eq!(cap.limited_by, "page_cache_max_bytes");
}

#[test]
fn missing_memory_evidence_never_guesses_a_budget() {
    let budget = calculate_target(
        u64::MAX,
        WarmupPolicy {
            level: 10,
            max_bytes: u64::MAX,
            memory_reserve: 0,
        },
        None,
    );
    assert_eq!(budget.target, 0);
    assert_eq!(budget.limited_by, "memory_evidence_unavailable");
}

#[test]
fn cgroup_budget_uses_current_process_membership_and_tightest_limit() {
    let dir = tempfile::tempdir().unwrap();
    let proc_cgroup = dir.path().join("proc-self-cgroup");
    let mount = dir.path().join("cgroup");
    let service = mount.join("radixdb.slice/soak.service");
    std::fs::create_dir_all(&service).unwrap();
    std::fs::write(&proc_cgroup, "0::/radixdb.slice/soak.service\n").unwrap();
    std::fs::write(service.join("memory.current"), "300\n").unwrap();
    std::fs::write(service.join("memory.max"), "1000\n").unwrap();
    std::fs::write(service.join("memory.high"), "800\n").unwrap();

    assert_eq!(
        cgroup_v2_available_memory_bytes_from(&proc_cgroup, &mount),
        Some(500)
    );
}

#[test]
fn unlimited_cgroup_does_not_invent_a_finite_limit() {
    let dir = tempfile::tempdir().unwrap();
    let proc_cgroup = dir.path().join("proc-self-cgroup");
    let mount = dir.path().join("cgroup");
    std::fs::create_dir_all(&mount).unwrap();
    std::fs::write(&proc_cgroup, "0::/\n").unwrap();
    std::fs::write(mount.join("memory.current"), "300\n").unwrap();
    std::fs::write(mount.join("memory.max"), "max\n").unwrap();
    std::fs::write(mount.join("memory.high"), "max\n").unwrap();

    assert_eq!(
        cgroup_v2_available_memory_bytes_from(&proc_cgroup, &mount),
        None
    );
}

#[test]
fn levels_zero_one_five_and_ten_are_monotonic() {
    let targets = [0_u8, 1, 5, 10].map(|level| {
        calculate_target(
            10_003,
            WarmupPolicy {
                level,
                max_bytes: 20_000,
                memory_reserve: 1,
            },
            Some(20_001),
        )
        .target
    });
    assert_eq!(targets, [0, 1_001, 5_002, 10_003]);
    assert!(targets.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn unsafe_manifest_member_paths_are_rejected() {
    let root = Path::new("/tmp/radixdb-page-cache");
    assert!(resolve_generation_path(root, Path::new("artifacts/data/01/member.data")).is_ok());
    assert!(resolve_generation_path(root, Path::new("../escape.data")).is_err());
    assert!(resolve_generation_path(root, Path::new("/absolute.data")).is_err());
}

#[test]
fn warmup_priority_is_metadata_then_indexes_then_recent_data() {
    let dir = tempfile::tempdir().unwrap();
    let paths = ["metadata.mft", "lookup.idx", "old.data", "recent.data"];
    for path in paths {
        std::fs::write(dir.path().join(path), [1_u8]).unwrap();
    }
    let mut files = vec![
        warmup_file(dir.path().join("old.data"), WarmupFileKind::Data).unwrap(),
        warmup_file(dir.path().join("recent.data"), WarmupFileKind::Data).unwrap(),
        warmup_file(dir.path().join("lookup.idx"), WarmupFileKind::Index).unwrap(),
        warmup_file(dir.path().join("metadata.mft"), WarmupFileKind::Metadata).unwrap(),
    ];
    files[0].access_priority = 3;
    files[1].access_priority = 9;
    sort_warmup_files(&mut files);
    let ordered = files
        .iter()
        .map(|file| {
            file.path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ordered,
        ["metadata.mft", "lookup.idx", "recent.data", "old.data"]
    );
}

#[test]
fn warm_file_range_uses_bounded_buffer_and_exact_range() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("payload.data");
    std::fs::write(&path, vec![7_u8; READ_BUFFER_BYTES * 2 + 31]).unwrap();
    let stop = AtomicBool::new(false);
    let mut progress = 0_u64;
    let read = warm_file_range(&path, 17, (READ_BUFFER_BYTES + 19) as u64, &stop, |bytes| {
        progress += bytes
    })
    .unwrap();
    assert_eq!(read, (READ_BUFFER_BYTES + 19) as u64);
    assert_eq!(progress, read);
}

#[test]
fn warm_file_range_honors_cancellation_before_io() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("payload.data");
    std::fs::write(&path, vec![7_u8; 4096]).unwrap();
    let stop = AtomicBool::new(true);
    let mut progress = 0_u64;
    let error = warm_file_range(&path, 0, 4096, &stop, |bytes| progress += bytes)
        .expect_err("cancelled warmup must not read payload");
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(progress, 0);
}

#[test]
fn inventory_uses_generation_members_not_directory_enumeration() {
    let dir = tempfile::tempdir().unwrap();
    let engine = create_persistent_fixture(dir.path(), 1..=32);
    let generation = engine.pin_physical_generation_for_test();
    let before = collect_current_generation_files(dir.path(), generation.snapshot()).unwrap();

    let unrelated = dir.path().join("artifacts/retired");
    std::fs::create_dir_all(&unrelated).unwrap();
    std::fs::write(unrelated.join("unreachable.data"), [7_u8; 64]).unwrap();

    let after = collect_current_generation_files(dir.path(), generation.snapshot()).unwrap();
    assert_eq!(after.fingerprint, before.fingerprint);
    assert_eq!(after.total_bytes, before.total_bytes);
    assert_eq!(after.files.len(), before.files.len());
    assert!(after
        .files
        .iter()
        .all(|file| !file.path.starts_with(&unrelated)));
    drop(generation);
    engine.close_engine().unwrap();
}

#[test]
fn actual_levels_warm_without_mutating_durable_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = create_persistent_fixture(dir.path(), 1..=512);
    let generation = engine.pin_physical_generation_for_test();
    let inventory = collect_current_generation_files(dir.path(), generation.snapshot()).unwrap();
    let durable_before = inventory_bytes(&inventory);

    for level in [0_u8, 1, 5, 10] {
        let controller =
            PageCacheWarmupController::new(dir.path(), level, inventory.total_bytes, 1);
        controller.request(generation.clone());
        let mut handle = controller.start();
        if level == 0 {
            assert!(handle.is_none());
            let snapshot = controller.snapshot().unwrap();
            assert_eq!(snapshot.state, "disabled");
            assert_eq!(snapshot.warmed_bytes, 0);
        } else {
            assert!(controller.wait_until_idle(Duration::from_secs(10)));
            let snapshot = controller.snapshot().unwrap();
            let expected = inventory
                .total_bytes
                .saturating_mul(u64::from(level))
                .saturating_add(u64::from(MAX_PAGE_CACHE_LEVEL - 1))
                / u64::from(MAX_PAGE_CACHE_LEVEL);
            assert_eq!(snapshot.state, "complete");
            assert_eq!(snapshot.generation_fingerprint, inventory.fingerprint);
            assert_eq!(snapshot.target_bytes, expected);
            assert_eq!(snapshot.warmed_bytes, expected);
            handle.as_mut().unwrap().stop().unwrap();
        }
        assert_eq!(inventory_bytes(&inventory), durable_before);
    }
    drop(generation);
    engine.close_engine().unwrap();
}

#[test]
fn generation_replacement_reuses_unchanged_warmed_files() {
    let dir = tempfile::tempdir().unwrap();
    let engine = create_persistent_fixture(dir.path(), 1..=512);
    let controller = PageCacheWarmupController::new(dir.path(), 10, u64::MAX, 1);
    let before_generation = engine.pin_physical_generation_for_test();
    controller.request(before_generation.clone());
    let mut handle = controller.start().unwrap();
    assert!(controller.wait_until_idle(Duration::from_secs(10)));
    let before = controller.snapshot().unwrap();
    let before_warmed = controller.shared.runtime.lock().unwrap().warmed.clone();

    for id in 513..=1_024_i64 {
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table("warm_items").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(id),
                Value::text(format!("warmup-payload-{id:04}")),
            ]))
            .unwrap();
        drop(table);
        transaction.commit().unwrap();
    }
    engine.force_checkpoint_cycle().unwrap();

    let after_generation = engine.pin_physical_generation_for_test();
    controller.request(after_generation);
    assert!(controller.wait_until_idle(Duration::from_secs(10)));
    let after = controller.snapshot().unwrap();
    let after_warmed = controller.shared.runtime.lock().unwrap().warmed.clone();
    let reused_data = before_warmed
        .iter()
        .filter(|(path, previous)| {
            path.extension().and_then(|value| value.to_str()) == Some("data")
                && after_warmed.get(*path).is_some_and(|current| {
                    current.stamp == previous.stamp && current.bytes == previous.bytes
                })
        })
        .count();

    assert_ne!(after.generation_fingerprint, before.generation_fingerprint);
    assert!(after.total_generation_bytes > before.total_generation_bytes);
    assert!(reused_data > 0);
    handle.stop().unwrap();
    drop(before_generation);
    engine.close_engine().unwrap();
}
