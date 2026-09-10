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

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use radixdb::{Database, NamedParams, Value};
use radixdb_join_workload::{
    apply_schema, execute_case, execute_case_with_timeout, seed_database, CaseId, CaseResult,
    WorkloadScale,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FrozenCase {
    case: CaseId,
    rows: usize,
    checksum: String,
    bytes: u64,
}

#[test]
fn complete_corpus_is_stable_sequentially_and_under_4_and_16_clients() {
    let scale = WorkloadScale::smoke();
    let database = Database::open_in_memory().expect("open contention database");
    apply_schema(&database).expect("apply contention schema");
    seed_database(&database, scale).expect("seed contention corpus");

    let oracle = Arc::new(
        CaseId::ALL
            .into_iter()
            .map(|case| freeze(execute_case(&database, case, scale).unwrap()))
            .collect::<Vec<_>>(),
    );
    let steady_threads = process_thread_count();
    run_clients(&database, scale, 4, Arc::clone(&oracle));
    run_clients(&database, scale, 16, oracle);
    assert!(
        process_thread_count() <= steady_threads.saturating_add(2),
        "controlled contention leaked worker threads: before={steady_threads}, after={}",
        process_thread_count()
    );
    database.close().expect("close contention database");
}

#[test]
fn long_reader_keeps_one_statement_snapshot_across_concurrent_commit() {
    let scale = WorkloadScale::smoke();
    let reader = Database::open("memory://join-long-reader").expect("open reader");
    let writer = reader.clone();
    apply_schema(&reader).expect("apply snapshot schema");
    seed_database(&reader, scale).expect("seed snapshot corpus");

    let user_id = "00000000-0100-7000-8000-000000000000";
    let params = || NamedParams::new().add("user_id", user_id);
    let mut rows = reader
        .query_named(CaseId::Q5SnapshotUsers.sql(), params())
        .expect("open long JOIN reader");
    assert!(rows.advance(), "Q5 must publish a first row");
    let mut old_reader_names = vec![rows
        .current_row()
        .unwrap()
        .get(2)
        .cloned()
        .expect("Q5 display_name")];

    writer
        .execute(
            &format!(
                "UPDATE users SET display_name = 'MUTATED AFTER SNAPSHOT' WHERE id = '{user_id}'"
            ),
            (),
        )
        .expect("commit concurrent dictionary update");

    while rows.advance() {
        old_reader_names.push(
            rows.current_row()
                .unwrap()
                .get(2)
                .cloned()
                .expect("Q5 display_name"),
        );
    }
    assert!(rows.error().is_none(), "long JOIN reader failed");
    assert!(
        old_reader_names
            .iter()
            .all(|value| value != &Value::text("MUTATED AFTER SNAPSHOT")),
        "one statement mixed pre-commit and post-commit dictionary versions"
    );

    let mut fresh = reader
        .query_named(CaseId::Q5SnapshotUsers.sql(), params())
        .expect("open post-commit reader");
    let mut mutation_seen = false;
    while fresh.advance() {
        mutation_seen |=
            fresh.current_row().unwrap().get(2) == Some(&Value::text("MUTATED AFTER SNAPSHOT"));
    }
    assert!(fresh.error().is_none(), "post-commit reader failed");
    assert!(
        mutation_seen,
        "fresh statement did not observe committed update"
    );

    drop(fresh);
    drop(rows);
    drop(writer);
    reader.close().expect("close snapshot database");
}

fn run_clients(
    database: &Database,
    scale: WorkloadScale,
    clients: usize,
    oracle: Arc<Vec<FrozenCase>>,
) {
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut workers = Vec::with_capacity(clients);
    for client in 0..clients {
        let database = database.clone();
        let barrier = Arc::clone(&barrier);
        let oracle = Arc::clone(&oracle);
        workers.push(thread::spawn(move || -> Result<(), String> {
            barrier.wait();
            for case in CaseId::ALL {
                let actual = freeze(
                    execute_case_with_timeout(&database, case, scale, Duration::from_secs(30))
                        .map_err(|error| format!("client {client} {}: {error}", case.name()))?,
                );
                let expected = oracle
                    .iter()
                    .find(|expected| expected.case == case)
                    .expect("complete oracle");
                if &actual != expected {
                    return Err(format!(
                        "client {client} {} mismatch: expected {expected:#?}, got {actual:#?}",
                        case.name()
                    ));
                }
            }
            Ok(())
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().expect("contention worker panicked").unwrap();
    }
}

fn freeze(result: CaseResult) -> FrozenCase {
    FrozenCase {
        case: result.case,
        rows: result.rows,
        checksum: result.checksum_sha256,
        bytes: result.canonical_result_bytes,
    }
}

fn process_thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or(0)
}
