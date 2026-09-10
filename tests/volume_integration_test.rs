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

//! Integration tests for immutable artifact-backed storage.
//!
//! Tests the full lifecycle: insert → close → reopen → query → update → verify.

use radixdb::Database;

fn query_i64(db: &Database, sql: &str) -> i64 {
    let mut r = db.query(sql, ()).unwrap();
    r.next()
        .and_then(|r| r.ok())
        .and_then(|r| r.get::<i64>(0).ok())
        .unwrap_or(-1)
}

fn query_f64(db: &Database, sql: &str) -> f64 {
    let mut r = db.query(sql, ()).unwrap();
    r.next()
        .and_then(|r| r.ok())
        .and_then(|r| r.get::<f64>(0).ok())
        .unwrap_or(f64::NAN)
}

fn query_str(db: &Database, sql: &str) -> String {
    let mut r = db.query(sql, ()).unwrap();
    r.next()
        .and_then(|r| r.ok())
        .and_then(|r| r.get::<String>(0).ok())
        .unwrap_or_default()
}

fn query_count_rows(db: &Database, sql: &str) -> i64 {
    let mut r = db.query(sql, ()).unwrap();
    let mut count = 0i64;
    for _ in r.by_ref() {
        count += 1;
    }
    count
}

/// Create a test table with enough data to trigger immutable artifact publication.
fn setup_large_table(db: &Database, row_count: usize) {
    db.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            name TEXT NOT NULL,
            price FLOAT NOT NULL,
            quantity INTEGER NOT NULL,
            active BOOLEAN NOT NULL,
            description TEXT NOT NULL
        )",
        (),
    )
    .unwrap();

    let categories = ["electronics", "books", "clothing", "food", "toys"];

    // Use a transaction for bulk insert (much faster than individual auto-commits)
    db.execute("BEGIN", ()).unwrap();
    let stmt = db
        .prepare("INSERT INTO items VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .unwrap();
    for i in 0..row_count {
        let cat = categories[i % categories.len()];
        // Use unique padding per row to defeat LZ4 compression
        // (keeps the fixture well above the ordinary seal threshold)
        let desc = format!("item_{}_desc_{:0>400}", i, i);
        stmt.execute((
            i as i64,
            cat,
            format!("item_{}", i),
            10.0 + (i as f64 * 0.1),
            (i % 100) as i64,
            i % 3 != 0,
            &desc,
        ))
        .unwrap();
    }
    db.execute("COMMIT", ()).unwrap();
}

/// Force a checkpoint (publish hot rows, advance WAL floor) and close.
fn snapshot_and_close(db: Database) {
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    db.close().unwrap();
}

fn count_data_artifacts(dir: &std::path::Path, db_name: &str) -> usize {
    fn count_below(path: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(path) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .map(|path| {
                if path.is_dir() {
                    count_below(&path)
                } else {
                    usize::from(path.extension().is_some_and(|suffix| suffix == "data"))
                }
            })
            .sum()
    }

    count_below(&dir.join(db_name).join("artifacts").join("data"))
}

fn has_data_artifact(dir: &std::path::Path, db_name: &str) -> bool {
    count_data_artifacts(dir, db_name) > 0
}

fn pseudo_random_payload(seed: i64, chunks: usize) -> String {
    let mut state = seed as u64 ^ 0x9E37_79B9_7F4A_7C15;
    let mut payload = String::with_capacity(chunks * 16);
    for _ in 0..chunks {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push_str(&format!("{:016x}", state));
    }
    payload
}

#[test]
fn test_volume_basic_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_basic", dir.path().display());

    // Session 1: Insert data
    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 200_000);
        snapshot_and_close(db);
    }

    // Session 2: Reopen from the published artifact generation.
    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 200_000);
        db.close().unwrap();
    }

    // Verify authoritative row data was published in the current artifact tree.
    assert!(
        has_data_artifact(dir.path(), "vol_basic"),
        "expected a .data artifact for the large table after checkpoint"
    );

    // Session 3: Reopen from the same immutable generation.
    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 200_000);

        // Test various queries
        let distinct = query_i64(&db, "SELECT COUNT(DISTINCT category) FROM items");
        assert_eq!(distinct, 5);

        let max_price = query_f64(&db, "SELECT MAX(price) FROM items");
        assert!(
            max_price > 100.0,
            "max_price should be > 100, got {}",
            max_price
        );

        let min_price = query_f64(&db, "SELECT MIN(price) FROM items");
        assert!((min_price - 10.0).abs() < 0.01);

        db.close().unwrap();
    }
}

#[test]
fn test_volume_aggregation_pushdown() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_agg", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();

        // These should use pre-computed volume stats (instant)
        let count = query_i64(&db, "SELECT COUNT(*) FROM items");
        assert_eq!(count, 200_000);

        let max = query_f64(&db, "SELECT MAX(price) FROM items");
        assert!(max > 0.0);

        let min = query_f64(&db, "SELECT MIN(price) FROM items");
        assert!(min >= 10.0);

        let sum = query_f64(&db, "SELECT SUM(price) FROM items");
        assert!(sum > 0.0);

        let avg = query_f64(&db, "SELECT AVG(price) FROM items");
        assert!(avg > 0.0);

        db.close().unwrap();
    }
}

#[test]
fn test_volume_filtered_queries() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_filter", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();

        // Filter by category
        let electronics = query_i64(
            &db,
            "SELECT COUNT(*) FROM items WHERE category = 'electronics'",
        );
        assert_eq!(electronics, 40_000); // 200K / 5 categories

        // Filter by price range
        let expensive = query_i64(&db, "SELECT COUNT(*) FROM items WHERE price > 500");
        assert!(expensive > 0, "should have items with price > 500");

        // Multi-column filter
        let active_books = query_i64(
            &db,
            "SELECT COUNT(*) FROM items WHERE category = 'books' AND active = true",
        );
        assert!(active_books > 0);

        // GROUP BY
        let groups = query_count_rows(
            &db,
            "SELECT category, COUNT(*), AVG(price) FROM items GROUP BY category",
        );
        assert_eq!(groups, 5);

        db.close().unwrap();
    }
}

#[test]
fn test_volume_wal_replay_updates() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_wal", dir.path().display());

    // Session 1: Insert initial data
    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    // Session 2: Reopen and modify some rows
    {
        let db = Database::open(&dsn).unwrap();

        // Update some rows
        db.execute("UPDATE items SET price = 99999.99 WHERE id = 0", ())
            .unwrap();
        db.execute("UPDATE items SET price = 88888.88 WHERE id = 1", ())
            .unwrap();

        // Delete some rows
        db.execute("DELETE FROM items WHERE id = 2", ()).unwrap();
        db.execute("DELETE FROM items WHERE id = 3", ()).unwrap();

        // Insert new rows
        db.execute(
            "INSERT INTO items VALUES (200000, 'new', 'new_item', 777.77, 1, true, 'desc')",
            (),
        )
        .unwrap();

        // Verify within same session
        let price0 = query_f64(&db, "SELECT price FROM items WHERE id = 0");
        assert!((price0 - 99999.99).abs() < 0.01);

        let count = query_i64(&db, "SELECT COUNT(*) FROM items");
        assert_eq!(count, 199_999); // 200K - 2 deleted + 1 inserted

        db.close().unwrap();
    }

    // Session 3: Reopen and verify WAL changes are visible
    {
        let db = Database::open(&dsn).unwrap();

        // Updated rows should have new values (from WAL, tombstoning volume rows)
        let price0 = query_f64(&db, "SELECT price FROM items WHERE id = 0");
        assert!(
            (price0 - 99999.99).abs() < 0.01,
            "Updated price should be 99999.99, got {}",
            price0
        );

        let price1 = query_f64(&db, "SELECT price FROM items WHERE id = 1");
        assert!(
            (price1 - 88888.88).abs() < 0.01,
            "Updated price should be 88888.88, got {}",
            price1
        );

        // Deleted rows should not exist
        let deleted = query_i64(&db, "SELECT COUNT(*) FROM items WHERE id = 2");
        assert_eq!(deleted, 0, "Deleted row id=2 should not exist");

        let deleted = query_i64(&db, "SELECT COUNT(*) FROM items WHERE id = 3");
        assert_eq!(deleted, 0, "Deleted row id=3 should not exist");

        // New row should exist
        let new_price = query_f64(&db, "SELECT price FROM items WHERE id = 200000");
        assert!(
            (new_price - 777.77).abs() < 0.01,
            "New row price should be 777.77, got {}",
            new_price
        );

        // Total count should match
        let count = query_i64(&db, "SELECT COUNT(*) FROM items");
        assert_eq!(count, 199_999);

        db.close().unwrap();
    }
}

#[test]
fn test_volume_group_by_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_group", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();

        // GROUP BY should produce correct results from volume data
        let mut r = db
            .query(
                "SELECT category, COUNT(*) as cnt FROM items GROUP BY category ORDER BY category",
                (),
            )
            .unwrap();

        let mut total = 0i64;
        for row_result in r.by_ref() {
            let row = row_result.unwrap();
            let cnt = row.get::<i64>(1).unwrap();
            assert_eq!(cnt, 40_000); // 200K / 5 categories = 40K each
            total += cnt;
        }
        assert_eq!(total, 200_000);

        db.close().unwrap();
    }
}

#[test]
fn test_volume_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_order", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();

        // ORDER BY + LIMIT should work correctly
        let top_price = query_f64(&db, "SELECT price FROM items ORDER BY price DESC LIMIT 1");
        let max_price = query_f64(&db, "SELECT MAX(price) FROM items");
        assert!(
            (top_price - max_price).abs() < 0.01,
            "ORDER BY DESC LIMIT 1 should match MAX: {} vs {}",
            top_price,
            max_price
        );

        // ORDER BY ASC
        let bottom_price = query_f64(&db, "SELECT price FROM items ORDER BY price ASC LIMIT 1");
        let min_price = query_f64(&db, "SELECT MIN(price) FROM items");
        assert!(
            (bottom_price - min_price).abs() < 0.01,
            "ORDER BY ASC LIMIT 1 should match MIN: {} vs {}",
            bottom_price,
            min_price
        );

        db.close().unwrap();
    }
}

#[test]
fn test_volume_truncate_drops_volumes() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_trunc", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 200_000);

        db.execute("TRUNCATE TABLE items", ()).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 0);

        // Insert new data after truncate
        db.execute(
            "INSERT INTO items VALUES (1, 'new', 'after_truncate', 42.0, 1, true, 'desc')",
            (),
        )
        .unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 1);

        db.close().unwrap();
    }

    // Reopen — truncated volumes should not reappear, only the post-truncate insert
    {
        let db = Database::open(&dsn).unwrap();
        let count = query_i64(&db, "SELECT COUNT(*) FROM items");
        assert_eq!(
            count, 1,
            "After truncate+insert+reopen, expected exactly 1 row, got {}",
            count
        );
        db.close().unwrap();
    }
}

#[test]
fn test_volume_drop_table_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_drop", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        setup_large_table(&db, 200_000);
        snapshot_and_close(db);
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 200_000);

        db.execute("DROP TABLE items", ()).unwrap();

        // Table should not exist
        let result = db.query("SELECT COUNT(*) FROM items", ());
        assert!(result.is_err());

        // Recreate with same name
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)", ())
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'fresh')", ())
            .unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM items"), 1);

        db.close().unwrap();
    }

    // Reopen — old volume data should NOT reappear
    {
        let db = Database::open(&dsn).unwrap();
        let count = query_i64(&db, "SELECT COUNT(*) FROM items");
        assert_eq!(
            count, 1,
            "After drop+recreate+insert+reopen, expected 1 row, got {}",
            count
        );
        let val = query_str(&db, "SELECT val FROM items WHERE id = 1");
        assert_eq!(
            val, "fresh",
            "Should see the fresh row, not old volume data"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_drop_recreate_uses_catalog_generation_without_table_local_manifest_authority() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("vol_drop_recreate_manifest");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'old@example.test')", ())
            .unwrap();
        db.close().unwrap();
    }

    assert!(db_path.join("CONTROL.0").exists() || db_path.join("CONTROL.1").exists());
    assert!(!db_path.join("volumes").exists());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("DROP TABLE IF EXISTS users", ()).unwrap();
        db.close().unwrap();
    }

    assert!(!db_path.join("volumes").exists());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("DROP TABLE IF EXISTS users", ()).unwrap();
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'new@example.test')", ())
            .unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM users"), 1);
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM users"),
            1,
            "recreated table rows must survive reopen after prior DROP cleanup"
        );
        assert_eq!(
            query_str(&db, "SELECT email FROM users WHERE id = 2"),
            "new@example.test"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_volume_small_table_stays_in_memory() {
    // Verify a small table survives regardless of whether checkpoint keeps it
    // hot or publishes an immutable artifact.
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/vol_small", dir.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, val TEXT)", ())
            .unwrap();
        for i in 0..100 {
            db.execute(
                &format!("INSERT INTO small VALUES ({}, 'val_{}')", i, i),
                (),
            )
            .unwrap();
        }
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM small"), 100);
        assert_eq!(
            query_str(&db, "SELECT val FROM small WHERE id = 42"),
            "val_42"
        );
        db.close().unwrap();
    }
}

#[test]
fn test_volume_restart_loads_multiple_volumes() {
    // HOT_ROWS use payloads sized to exceed the 16MB volume threshold on reopen.
    // 1000 rows * ~20KB payload = ~20MB > 16MB threshold.
    const SEALED_ROWS: i64 = 100_000;
    const HOT_ROWS: i64 = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/vol_mixed_restart?checkpoint_interval=3600&cleanup_interval=3600&wal_compression=off&checkpoint_on_close=off",
        dir.path().display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                note TEXT NOT NULL
            )",
            (),
        )
        .unwrap();

        db.execute("BEGIN", ()).unwrap();
        let stmt = db.prepare("INSERT INTO items VALUES ($1, $2, $3)").unwrap();
        for i in 0..SEALED_ROWS {
            stmt.execute((i, "sealed", "base")).unwrap();
        }
        db.execute("COMMIT", ()).unwrap();

        // Checkpoint publishes the first immutable DATA artifact.
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert!(
            has_data_artifact(dir.path(), "vol_mixed_restart"),
            "checkpoint should create a DATA artifact for sealed rows"
        );
        let first_generation_artifacts = count_data_artifacts(dir.path(), "vol_mixed_restart");

        db.execute("BEGIN", ()).unwrap();
        for i in SEALED_ROWS..(SEALED_ROWS + HOT_ROWS) {
            let payload = pseudo_random_payload(i, 1280);
            stmt.execute((i, "hot", payload)).unwrap();
        }
        db.execute("COMMIT", ()).unwrap();

        // Publish the hot tail without replacing the first immutable artifact.
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert!(
            count_data_artifacts(dir.path(), "vol_mixed_restart") > first_generation_artifacts,
            "the hot tail must publish an additional DATA artifact"
        );

        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM items"),
            SEALED_ROWS + HOT_ROWS,
            "restart should load both immutable artifact generations"
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM items WHERE category = 'sealed'"),
            SEALED_ROWS
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM items WHERE category = 'hot'"),
            HOT_ROWS
        );
        db.close().unwrap();
    }
}
