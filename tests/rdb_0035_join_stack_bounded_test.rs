// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! RDB-0035: a valid production-sized JOIN tree must fit on the standard
//! server worker stack in an unoptimized build.

use radixdb::Database;

const STANDARD_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;
const RELATION_COUNT: usize = 18;

#[test]
fn seventeen_join_edges_fit_the_standard_worker_stack() {
    std::thread::Builder::new()
        .name("rdb-0035-standard-stack".to_owned())
        .stack_size(STANDARD_WORKER_STACK_BYTES)
        .spawn(|| {
            let db = Database::open("memory://rdb_0035_join_stack_bounded").unwrap();
            for relation in 0..RELATION_COUNT {
                let create = format!("CREATE TABLE rdb35_j{relation:02} (id INTEGER PRIMARY KEY)");
                db.execute(&create, ()).unwrap();
                let insert = format!("INSERT INTO rdb35_j{relation:02} VALUES (1)");
                db.execute(&insert, ()).unwrap();
            }

            let mut query = "SELECT j00.id FROM rdb35_j00 AS j00".to_owned();
            for relation in 1..RELATION_COUNT {
                query.push_str(&format!(
                    " INNER JOIN rdb35_j{relation:02} AS j{relation:02} \
                     ON j{relation:02}.id = j00.id"
                ));
            }
            query.push_str(" WHERE j00.id = 1");

            let rows = db
                .query(&query, ())
                .unwrap()
                .map(|row| row.unwrap())
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 1);
            let id: i64 = rows[0].get(0).unwrap();
            assert_eq!(id, 1);
        })
        .unwrap()
        .join()
        .unwrap();
}
