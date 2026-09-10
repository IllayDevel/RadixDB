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

use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

use radixdb::storage::instrumentation;
use radixdb::Database;
use radixdb_join_workload::{
    apply_schema, execute_case, explain_case, seed_database, CaseId, CaseResult, WorkloadScale,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FrozenCase {
    case: CaseId,
    rows: usize,
    checksum: String,
    bytes: u64,
}

#[test]
fn corpus_is_identical_across_hot_cold_hybrid_reopen_compaction_and_cache_levels() {
    let scale = WorkloadScale::smoke();
    let memory = Database::open_in_memory().expect("open memory oracle");
    apply_schema(&memory).expect("apply memory schema");
    seed_database(&memory, scale).expect("seed memory oracle");
    let oracle = capture_corpus(&memory, scale);
    memory.close().expect("close memory oracle");

    let directory = tempfile::tempdir().expect("create storage-mode directory");
    let database_path = directory.path().join("join-storage-modes");
    let level_zero = dsn(&database_path, 0);
    let database = Database::open(&level_zero).expect("open file-hot database");
    apply_schema(&database).expect("apply file schema");
    seed_database(&database, scale).expect("seed file-hot corpus");
    assert_corpus("file-hot", &oracle, &capture_corpus(&database, scale));

    database
        .execute("PRAGMA CHECKPOINT", ())
        .expect("publish initial cold corpus");
    database.close().expect("close before DONTNEED");
    let (evicted_files, evicted_bytes) =
        evict_database_page_cache(&database_path).expect("evict database page cache");
    assert!(evicted_files > 0 && evicted_bytes > 0);

    let database = Database::open(&level_zero).expect("reopen cold database");
    instrumentation::reset();
    assert_corpus("DONTNEED-cold", &oracle, &capture_corpus(&database, scale));
    let cold_counters = instrumentation::snapshot();
    assert!(
        cold_counters.artifact_pread_calls > 0 || cold_counters.volume_read_calls > 0,
        "cold gate did not perform immutable-storage reads: {cold_counters:#?}"
    );
    assert_fused_index_lookup_materialization(&database, scale);

    insert_unrelated_graph(&database, 0);
    assert_corpus(
        "hybrid-artifact-backed-plus-hot",
        &oracle,
        &capture_corpus(&database, scale),
    );

    instrumentation::reset();
    for generation in 1..=4 {
        insert_unrelated_graph(&database, generation);
        database
            .execute("PRAGMA CHECKPOINT", ())
            .unwrap_or_else(|error| panic!("checkpoint generation {generation}: {error}"));
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while instrumentation::snapshot().compaction_calls == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        instrumentation::snapshot().compaction_calls > 0,
        "storage-mode gate never observed background compaction"
    );
    assert_corpus(
        "post-compaction",
        &oracle,
        &capture_corpus(&database, scale),
    );
    database
        .execute("PRAGMA CHECKPOINT", ())
        .expect("publish post-compaction state");
    database.close().expect("close post-compaction database");

    let database = Database::open(&level_zero).expect("reopen level-0 database");
    let level_zero_plans = capture_plans(&database, scale);
    assert_corpus(
        "reopen-page-cache-0",
        &oracle,
        &capture_corpus(&database, scale),
    );
    database.close().expect("close level-0 database");

    let level_ten = dsn(&database_path, 10);
    let database = Database::open(&level_ten).expect("reopen level-10 database");
    database.engine().request_page_cache_warmup();
    assert!(
        database
            .engine()
            .wait_for_page_cache_warmup(Duration::from_secs(10)),
        "page-cache warmup did not become idle"
    );
    assert_corpus(
        "reopen-page-cache-10",
        &oracle,
        &capture_corpus(&database, scale),
    );
    let level_ten_plans = capture_plans(&database, scale);
    assert_eq!(
        level_ten_plans, level_zero_plans,
        "page-cache policy changed the physical JOIN plans"
    );
    database.close().expect("close level-10 database");
}

fn dsn(path: &Path, page_cache_level: u8) -> String {
    format!(
        "file://{}?checkpoint_interval=0&checkpoint_on_close=off&compact_threshold=2&max_compaction_jobs=2&page_cache_level={page_cache_level}&page_cache_memory_reserve=1",
        path.display()
    )
}

fn capture_corpus(database: &Database, scale: WorkloadScale) -> Vec<FrozenCase> {
    CaseId::ALL
        .into_iter()
        .map(|case| {
            freeze(
                execute_case(database, case, scale)
                    .unwrap_or_else(|error| panic!("{} execution failed: {error}", case.name())),
            )
        })
        .collect()
}

fn freeze(result: CaseResult) -> FrozenCase {
    FrozenCase {
        case: result.case,
        rows: result.rows,
        checksum: result.checksum_sha256,
        bytes: result.canonical_result_bytes,
    }
}

fn assert_corpus(label: &str, expected: &[FrozenCase], actual: &[FrozenCase]) {
    assert_eq!(actual, expected, "corpus mismatch in {label}");
}

fn capture_plans(database: &Database, scale: WorkloadScale) -> Vec<(CaseId, Vec<String>)> {
    CaseId::ALL
        .into_iter()
        .map(|case| {
            let plan = explain_case(database, case, scale)
                .unwrap_or_else(|error| panic!("{} EXPLAIN failed: {error}", case.name()));
            (case, plan)
        })
        .collect()
}

fn assert_fused_index_lookup_materialization(database: &Database, scale: WorkloadScale) {
    for case in [CaseId::Q5SnapshotUsers, CaseId::Q6SnapshotMembers] {
        instrumentation::reset();
        let result = execute_case(database, case, scale)
            .unwrap_or_else(|error| panic!("{} fused lookup failed: {error}", case.name()));
        assert_eq!(result.rows, scale.expected_rows(case));
        let counters = instrumentation::snapshot();
        assert!(
            counters.join_lookup_candidate_rows > 0,
            "{} did not exercise its persisted indexed lookup: {counters:#?}",
            case.name()
        );
        assert!(
            counters.row_materialization_rows
                <= counters
                    .join_lookup_candidate_rows
                    .saturating_add(scale.users as u64),
            "{} rematerialized persisted lookup candidates instead of fusing the authoritative recheck with projection: {counters:#?}",
            case.name()
        );
    }
}

fn insert_unrelated_graph(database: &Database, generation: usize) {
    let suffix = generation + 1;
    let user = format!("ffff0000-0001-7000-8000-{suffix:012x}");
    let device = format!("ffff0000-0002-7000-8000-{suffix:012x}");
    let session = format!("ffff0000-0003-7000-8000-{suffix:012x}");
    let token = format!("ffff0000-0004-7000-8000-{suffix:012x}");
    let job = format!("ffff0000-0005-7000-8000-{suffix:012x}");
    let statements = [
        format!(
            "INSERT INTO users VALUES ('{user}', 'unrelated-{suffix}', 'Unrelated {suffix}', 'human', NULL, 1, NULL, NULL)"
        ),
        format!(
            "INSERT INTO devices VALUES ('{device}', '{user}', '2026-08-28 11:00:00', NULL)"
        ),
        format!(
            "INSERT INTO sessions VALUES ('{session}', '{user}', '{device}', '2027-08-28 12:00:00', '2027-08-28 12:00:00', NULL)"
        ),
        format!(
            "INSERT INTO push_tokens VALUES ('{token}', '{user}', '{device}', 'webpush', 'unrelated-{suffix}', NULL, NULL, NULL, 1)"
        ),
        format!(
            "INSERT INTO outbox_jobs VALUES ('{job}', 'unrelated', 'unrelated', '{user}', 'done', '2026-08-28 12:00:00', NULL, 1, '2026-08-28 12:00:00')"
        ),
    ];
    for statement in statements {
        database
            .execute(&statement, ())
            .unwrap_or_else(|error| panic!("unrelated generation {generation}: {error}"));
    }
}

fn evict_database_page_cache(root: &Path) -> std::io::Result<(u64, u64)> {
    let canonical_root = root.canonicalize()?;
    let mut pending = vec![canonical_root.clone()];
    let mut files = 0u64;
    let mut bytes = 0u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(std::io::Error::other(format!(
                    "refusing DONTNEED through symlink {}",
                    path.display()
                )));
            }
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let canonical = path.canonicalize()?;
            if !canonical.starts_with(&canonical_root) {
                return Err(std::io::Error::other(format!(
                    "database member escaped root: {}",
                    canonical.display()
                )));
            }
            let file = File::open(canonical)?;
            // SAFETY: the file descriptor remains valid for the complete call.
            let result =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            if result != 0 {
                return Err(std::io::Error::from_raw_os_error(result));
            }
            files = files.saturating_add(1);
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((files, bytes))
}
