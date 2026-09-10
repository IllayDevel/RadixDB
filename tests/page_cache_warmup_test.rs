// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Facade-level page-cache warmup contracts.

use std::time::Duration;

use radixdb::api::Database;

#[test]
fn engine_warms_only_bounded_current_generation_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=0&page_cache_level=10&page_cache_max_bytes=65536&page_cache_memory_reserve=1",
        dir.path().display()
    );
    let db = Database::open(&dsn).unwrap();
    db.execute(
        "CREATE TABLE warm_items (id INTEGER PRIMARY KEY, payload TEXT)",
        (),
    )
    .unwrap();
    for id in 1..=512_i64 {
        db.execute(
            "INSERT INTO warm_items (id, payload) VALUES ($1, $2)",
            (id, format!("warmup-payload-{id:04}")),
        )
        .unwrap();
    }
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    db.engine().request_page_cache_warmup();
    assert!(db
        .engine()
        .wait_for_page_cache_warmup(Duration::from_secs(10)));
    let snapshot = db.engine().page_cache_warmup_snapshot().unwrap();
    assert_eq!(snapshot.state, "complete");
    assert!(snapshot.total_generation_bytes > 0);
    assert!(snapshot.target_bytes > 0);
    assert!(snapshot.target_bytes <= 65_536);
    assert_eq!(snapshot.warmed_bytes, snapshot.target_bytes);
    assert!(snapshot.resident_estimate_bytes <= snapshot.target_bytes);
    db.close().unwrap();
}
