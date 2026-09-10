// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Executable ownership gates for the physical `radixdb-storage` cut.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_below(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_below(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn storage_crate_has_only_the_approved_internal_dependency() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("crates/radixdb-storage/Cargo.toml"))
        .expect("read radixdb-storage manifest");
    let internal_dependencies = manifest
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("radixdb-") && line.contains('='))
        .map(|line| line.split('=').next().expect("dependency name").trim())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        internal_dependencies,
        BTreeSet::from(["radixdb-catalog", "radixdb-core"])
    );
    for forbidden in ["radixdb-sql", "radixdb-functions", "radixdb-executor"] {
        assert!(!manifest.contains(forbidden), "forbidden edge: {forbidden}");
    }
}

#[test]
fn storage_crate_sources_do_not_reach_root_or_upper_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let storage_root = root.join("crates/radixdb-storage/src");
    let mut sources = Vec::new();
    rust_sources_below(&storage_root, &mut sources);
    sources.sort();

    let forbidden = [
        "crate::core",
        "crate::common",
        "crate::storage",
        "radixdb_sql",
        "radixdb_functions",
        "radixdb_executor",
        "radixdb::",
    ];
    let mut violations = Vec::new();
    for path in sources {
        let source = fs::read_to_string(&path).expect("read storage crate source");
        for pattern in forbidden {
            if source.contains(pattern) {
                violations.push(format!("{} contains {pattern}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "radixdb-storage crossed its lower boundary:\n{}",
        violations.join("\n")
    );
}

#[test]
fn root_storage_mirror_tree_was_physically_removed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let library = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");
    assert!(library.contains("pub use radixdb_storage as storage;"));
    assert!(!root.join("src/storage").exists());
    assert!(!root.join("src/common/buffer_pool.rs").exists());
    assert!(!root.join("src/test_failpoints.rs").exists());
}

#[test]
fn canonical_storage_tree_contains_the_only_implementation_items() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let storage_root = root.join("crates/radixdb-storage/src");
    let mut sources = Vec::new();
    rust_sources_below(&storage_root, &mut sources);
    sources.sort();
    assert!(
        sources.len() > 40,
        "canonical storage owner unexpectedly empty"
    );
    assert!(!root.join("src/storage").exists());
}

#[test]
fn root_and_storage_crate_share_config_type_identity() {
    fn accept_canonical(_: radixdb_storage::Config) {}

    let through_root: radixdb::storage::Config = radixdb::storage::Config::in_memory();
    accept_canonical(through_root);
}

#[test]
fn moved_collections_and_expressions_keep_one_type_identity() {
    fn accept_set(_: radixdb_core::I64Set) {}
    fn accept_expression(_: Box<dyn radixdb_storage::expression::Expression>) {}

    let through_root_set: radixdb::I64Set = radixdb::I64Set::new();
    accept_set(through_root_set);

    let through_root_expression: Box<dyn radixdb::storage::Expression> =
        Box::new(radixdb::storage::ComparisonExpr::new(
            "id",
            radixdb::Operator::Eq,
            radixdb::Value::Integer(1),
        ));
    accept_expression(through_root_expression);
}

#[test]
fn moved_indexes_keep_one_trait_and_concrete_type_identity() {
    fn accept_index(_: Box<dyn radixdb_storage::Index>) {}
    fn accept_pk(_: radixdb_storage::PkIndex) {}

    let through_root: Box<dyn radixdb::storage::Index> = Box::new(radixdb::storage::PkIndex::new(
        "pk".into(),
        "items".into(),
        0,
        "id".into(),
    ));
    accept_index(through_root);

    let through_root_pk: radixdb::storage::PkIndex =
        radixdb::storage::PkIndex::new("pk".into(), "items".into(), 0, "id".into());
    accept_pk(through_root_pk);
}

#[test]
fn moved_artifact_and_streaming_contracts_keep_one_type_identity() {
    fn accept_buffer_pool(_: radixdb_storage::BufferPool) {}
    fn accept_data_header(_: Option<radixdb_storage::v6::DataArtifactHeader>) {}
    fn accept_volume(_: Option<radixdb_storage::volume::writer::FrozenVolume>) {}
    fn accept_source(_: Option<radixdb_storage::v6::ArtifactDataSource>) {}
    fn accept_scanner(_: Box<dyn radixdb_storage::Scanner>) {}

    accept_buffer_pool(radixdb::common::BufferPool::new(64, 4096, "identity"));

    let data_header: Option<radixdb::storage::v6::DataArtifactHeader> = None;
    accept_data_header(data_header);
    let volume: Option<radixdb::storage::volume::writer::FrozenVolume> = None;
    accept_volume(volume);
    let source: Option<radixdb::storage::v6::ArtifactDataSource> = None;
    accept_source(source);

    let scanner: Box<dyn radixdb::storage::Scanner> =
        Box::new(radixdb::storage::traits::EmptyScanner::new());
    accept_scanner(scanner);
}

#[test]
fn moved_manifest_and_timestamp_keep_one_type_identity() {
    fn accept_manifest(_: radixdb_storage::volume::manifest::TableManifest) {}
    fn accept_manager(_: Option<radixdb_storage::volume::manifest::SegmentManager>) {}
    fn accept_timestamp(_: fn() -> i64) {}

    let manifest: radixdb::storage::volume::manifest::TableManifest =
        radixdb::storage::volume::manifest::TableManifest::new("items");
    accept_manifest(manifest);

    let manager: Option<radixdb::storage::volume::manifest::SegmentManager> = None;
    accept_manager(manager);
    accept_timestamp(radixdb::storage::mvcc::timestamp::get_fast_timestamp);
}

#[test]
fn moved_table_scanner_and_zone_map_keep_one_type_identity() {
    fn accept_table(_: Option<Box<dyn radixdb_storage::Table>>) {}
    fn accept_segmented(_: Option<radixdb_storage::volume::table::SegmentedTable>) {}
    fn accept_volume_scanner(_: Option<radixdb_storage::volume::scanner::VolumeScanner>) {}
    fn accept_merge_scanner(_: Option<radixdb_storage::volume::scanner::MergingScanner>) {}
    fn accept_zone_map(_: Option<radixdb_storage::volume::zonemap::TableZoneMap>) {}
    fn accept_aggregate(_: radixdb_storage::AggregateOp) {}

    let table: Option<Box<dyn radixdb::storage::Table>> = None;
    accept_table(table);
    let segmented: Option<radixdb::storage::volume::table::SegmentedTable> = None;
    accept_segmented(segmented);
    let scanner: Option<radixdb::storage::volume::scanner::VolumeScanner> = None;
    accept_volume_scanner(scanner);
    let merger: Option<radixdb::storage::volume::scanner::MergingScanner> = None;
    accept_merge_scanner(merger);
    let zone_map: Option<radixdb::storage::mvcc::zonemap::TableZoneMap> = None;
    accept_zone_map(zone_map);
    accept_aggregate(radixdb::storage::mvcc::AggregateOp::CountStar);
}

#[test]
fn moved_mvcc_state_owners_keep_one_type_identity() {
    fn accept_arena(_: Option<radixdb_storage::mvcc::RowArena>) {}
    fn accept_registry(_: Option<radixdb_storage::mvcc::TransactionRegistry>) {}
    fn accept_transaction(_: Option<radixdb_storage::mvcc::MvccTransaction>) {}
    fn accept_store(_: Option<radixdb_storage::mvcc::VersionStore>) {}

    let arena: Option<radixdb::storage::mvcc::arena::RowArena> = None;
    accept_arena(arena);
    let registry: Option<radixdb::storage::mvcc::TransactionRegistry> = None;
    accept_registry(registry);
    let transaction: Option<radixdb::storage::mvcc::MvccTransaction> = None;
    accept_transaction(transaction);
    let store: Option<radixdb::storage::mvcc::VersionStore> = None;
    accept_store(store);
}

#[test]
fn moved_durability_owners_keep_one_type_identity() {
    fn accept_persistence(_: Option<radixdb_storage::mvcc::PersistenceManager>) {}
    fn accept_wal(_: Option<radixdb_storage::mvcc::WALManager>) {}
    fn accept_entry(_: Option<radixdb_storage::mvcc::WALEntry>) {}
    fn accept_metadata(_: Option<radixdb_storage::mvcc::IndexMetadata>) {}

    let persistence: Option<radixdb::storage::mvcc::PersistenceManager> = None;
    accept_persistence(persistence);
    let wal: Option<radixdb::storage::mvcc::WALManager> = None;
    accept_wal(wal);
    let entry: Option<radixdb::storage::mvcc::WALEntry> = None;
    accept_entry(entry);
    let metadata: Option<radixdb::storage::mvcc::IndexMetadata> = None;
    accept_metadata(metadata);
}

#[test]
fn moved_engine_owners_keep_one_type_identity() {
    fn accept_engine(_: Option<radixdb_storage::mvcc::MVCCEngine>) {}
    fn accept_file_lock(_: Option<radixdb_storage::mvcc::FileLock>) {}
    fn accept_warmup(_: Option<radixdb_storage::PageCacheWarmupSnapshot>) {}
    fn accept_engine_contract(_: Option<Box<dyn radixdb_storage::Engine>>) {}

    let engine: Option<radixdb::storage::mvcc::MVCCEngine> = None;
    accept_engine(engine);
    let file_lock: Option<radixdb::storage::mvcc::file_lock::FileLock> = None;
    accept_file_lock(file_lock);
    let warmup: Option<radixdb::storage::PageCacheWarmupSnapshot> = None;
    accept_warmup(warmup);
    let contract: Option<Box<dyn radixdb::storage::Engine>> = None;
    accept_engine_contract(contract);
}

#[test]
fn moved_statistics_keep_one_type_identity() {
    fn accept_table_stats(_: Option<radixdb_storage::statistics::TableStats>) {}
    fn accept_histogram(_: Option<radixdb_storage::statistics::Histogram>) {}

    let table_stats: Option<radixdb::storage::statistics::TableStats> = None;
    accept_table_stats(table_stats);
    let histogram: Option<radixdb::storage::statistics::Histogram> = None;
    accept_histogram(histogram);
}

#[test]
fn aggregate_contract_has_no_second_mvcc_owner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source =
        fs::read_to_string(root.join("crates/radixdb-storage/src/mvcc/version_store/mod.rs"))
            .expect("read canonical MVCC version-store owner");
    for forbidden in [
        "pub enum AggregateOp",
        "pub enum GroupKey",
        "pub struct GroupedAggregateResult",
        "fn index_values_for_row(",
    ] {
        assert!(
            !source.contains(forbidden),
            "MVCC source recreated canonical storage contract {forbidden:?}"
        );
    }
}

#[test]
fn moved_volume_production_files_stay_within_documented_bounds() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let volume_root = root.join("crates/radixdb-storage/src/volume");
    let mut sources = Vec::new();
    rust_sources_below(&volume_root, &mut sources);

    let oversized = sources
        .into_iter()
        .filter(|path| !path.ends_with("tests.rs"))
        .filter_map(|path| {
            let lines = fs::read_to_string(&path).ok()?.lines().count();
            let accepted_ceiling = match path.file_name().and_then(|name| name.to_str()) {
                // EVO-50.4 explicitly froze these two pre-existing exceptions.
                // They may shrink in later lifecycle steps, but must not grow.
                Some("format.rs") => 3_033,
                Some("io.rs") => 3_396,
                _ => 3_000,
            };
            (lines > accepted_ceiling).then_some(format!(
                "{}: {lines} (accepted ceiling: {accepted_ceiling})",
                path.display()
            ))
        })
        .collect::<Vec<_>>();

    assert!(
        oversized.is_empty(),
        "moved volume production files exceeded their documented ceiling:\n{}",
        oversized.join("\n")
    );
}

#[test]
fn moved_index_production_files_stay_below_the_normal_size_ceiling() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let index_root = root.join("crates/radixdb-storage/src/index");
    let mut sources = Vec::new();
    rust_sources_below(&index_root, &mut sources);

    let oversized = sources
        .into_iter()
        .filter(|path| !path.ends_with("hnsw/tests.rs"))
        .filter_map(|path| {
            let lines = fs::read_to_string(&path).ok()?.lines().count();
            (lines > 3_000).then_some(format!("{}: {lines}", path.display()))
        })
        .collect::<Vec<_>>();

    assert!(
        oversized.is_empty(),
        "moved index production files exceeded 3000 lines:\n{}",
        oversized.join("\n")
    );
}

#[test]
fn moved_mvcc_state_files_stay_below_the_normal_size_ceiling() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mvcc_root = root.join("crates/radixdb-storage/src/mvcc");
    let mut sources = Vec::new();
    rust_sources_below(&mvcc_root, &mut sources);

    let oversized = sources
        .into_iter()
        .filter(|path| !path.ends_with("tests.rs"))
        .filter_map(|path| {
            let lines = fs::read_to_string(&path).ok()?.lines().count();
            (lines > 3_000).then_some(format!("{}: {lines}", path.display()))
        })
        .collect::<Vec<_>>();

    assert!(
        oversized.is_empty(),
        "moved MVCC state files exceeded 3000 lines:\n{}",
        oversized.join("\n")
    );
}
