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

use radixdb::Database;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn r8_l01_batch_i_write_pipeline_is_streaming_batched_and_key_scoped() {
    let dsn = "memory://r8_l01_batch_i_write_pipeline";
    let setup = Database::open(dsn).expect("open setup connection");
    setup
        .execute(
            "CREATE TABLE source (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            (),
        )
        .expect("create CTAS source");
    for chunk_start in (0..2_000).step_by(100) {
        let values = (chunk_start..chunk_start + 100)
            .map(|id| format!("({id}, {})", id * 2))
            .collect::<Vec<_>>()
            .join(",");
        setup
            .execute(&format!("INSERT INTO source VALUES {values}"), ())
            .expect("fill CTAS source");
    }

    // CTAS publishes the table and streamed rows under one transaction marker.
    assert_eq!(
        setup
            .execute(
                "CREATE TABLE derived AS SELECT id, value FROM source WHERE id >= 250",
                (),
            )
            .expect("stream CTAS"),
        1_750
    );
    let derived_count: i64 = setup
        .query_one("SELECT COUNT(*) FROM derived", ())
        .expect("count CTAS rows");
    assert_eq!(derived_count, 1_750);

    // RETURNING receives the exact outcome of one storage mutation batch.
    let mut deleted = setup
        .query(
            "DELETE FROM derived WHERE id % 2 = 0 RETURNING id, value",
            (),
        )
        .expect("batch DELETE RETURNING")
        .map(|row| {
            let row = row.expect("returning row");
            (
                row.get::<i64>(0).expect("returned id"),
                row.get::<i64>(1).expect("returned value"),
            )
        })
        .collect::<Vec<_>>();
    deleted.sort_unstable();
    assert_eq!(deleted.len(), 875);
    assert!(deleted
        .iter()
        .all(|(id, value)| id % 2 == 0 && *value == id * 2));
    let remaining: i64 = setup
        .query_one("SELECT COUNT(*) FROM derived", ())
        .expect("count remaining rows");
    assert_eq!(remaining, 875);

    setup
        .execute(
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            (),
        )
        .expect("create upsert table");
    setup
        .execute("INSERT INTO counters VALUES (1, 0), (2, 0)", ())
        .expect("seed upsert table");

    // Keep row 1 claimed, then start an UPSERT which waits on that exact row.
    // An independent-key UPSERT must not queue behind it.
    let owner = Database::open(dsn).expect("open claim owner");
    owner.execute("BEGIN", ()).expect("begin claim owner");
    owner
        .execute("UPDATE counters SET value = 10 WHERE id = 1", ())
        .expect("claim row 1");

    let blocked = Database::open(dsn).expect("open blocked upsert");
    let blocked_worker = thread::spawn(move || {
        blocked.execute(
            "INSERT INTO counters VALUES (1, 1) \
             ON CONFLICT (id) DO UPDATE SET value = counters.value + 1",
            (),
        )
    });
    thread::sleep(Duration::from_millis(100));

    let independent = Database::open(dsn).expect("open independent upsert");
    let (done_tx, done_rx) = mpsc::channel();
    let independent_worker = thread::spawn(move || {
        let result = independent.execute(
            "INSERT INTO counters VALUES (2, 1) \
             ON CONFLICT (id) DO UPDATE SET value = counters.value + 1",
            (),
        );
        done_tx.send(result).expect("report independent result");
    });

    let independent_result = done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("independent key must not wait behind row 1");
    assert_eq!(independent_result.expect("independent upsert succeeds"), 1);

    owner.execute("ROLLBACK", ()).expect("release row 1 claim");
    assert_eq!(
        blocked_worker
            .join()
            .expect("blocked worker joins")
            .expect("blocked upsert resumes"),
        1
    );
    independent_worker.join().expect("independent worker joins");

    let values = setup
        .query("SELECT id, value FROM counters ORDER BY id", ())
        .expect("read final counters")
        .map(|row| {
            let row = row.expect("counter row");
            (
                row.get::<i64>(0).expect("counter id"),
                row.get::<i64>(1).expect("counter value"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![(1, 1), (2, 1)]);
}
