// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0.

//! Self-contained Messenger `RDB-NNNN` prerelease regression belt.
//!
//! The neighboring Messenger repository is the historical source of these
//! scenarios, but this target deliberately carries no runtime dependency on it.

#![cfg(feature = "stress-tests")]

mod common;

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Barrier},
    thread,
};

use common::prerelease::{tcp_command, tcp_connect, tcp_scalar_i64, with_tcp_server, OwnedFixture};
use radixdb::Database;
use radixdb_client::{Connection, ExecuteResult};

const DATABASE: &str = "prerelease_b9";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Disposition {
    Regression,
    ExternalLifecycle,
    DeferredPerformance,
    OpenExternalAcceptance,
}

#[derive(Clone, Copy)]
struct TicketContract {
    id: u8,
    disposition: Disposition,
    owners: &'static [&'static str],
}

const TICKETS: &[TicketContract] = &[
    TicketContract {
        id: 1,
        disposition: Disposition::Regression,
        owners: &["tests/server_identity_test.rs"],
    },
    TicketContract {
        id: 2,
        disposition: Disposition::Regression,
        owners: &[
            "tests/ddl_transaction_test.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
    TicketContract {
        id: 3,
        disposition: Disposition::Regression,
        owners: &["tests/tcp_concurrent_row_update_test.rs"],
    },
    TicketContract {
        id: 4,
        disposition: Disposition::Regression,
        owners: &[
            "tests/timestamp_index_plan_test.rs",
            "tests/tcp_timestamp_index_plan_test.rs",
        ],
    },
    TicketContract {
        id: 5,
        disposition: Disposition::Regression,
        owners: &["tests/tcp_transaction_error_atomicity_test.rs"],
    },
    TicketContract {
        id: 6,
        disposition: Disposition::Regression,
        owners: &[
            "tests/ddl_transaction_test.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
    TicketContract {
        id: 7,
        disposition: Disposition::Regression,
        owners: &[
            "tests/multi_index_or_plan_test.rs",
            "tests/tcp_multi_index_or_plan_test.rs",
        ],
    },
    TicketContract {
        id: 8,
        disposition: Disposition::Regression,
        owners: &["tests/sequential_write_conflict_test.rs"],
    },
    TicketContract {
        id: 9,
        disposition: Disposition::Regression,
        owners: &["tests/table_check_constraint_test.rs"],
    },
    TicketContract {
        id: 10,
        disposition: Disposition::ExternalLifecycle,
        owners: &["tests/bound_uuid_in_test.rs"],
    },
    TicketContract {
        id: 11,
        disposition: Disposition::ExternalLifecycle,
        owners: &["tests/cold_composite_index_test.rs"],
    },
    TicketContract {
        id: 12,
        disposition: Disposition::ExternalLifecycle,
        owners: &["tests/cold_composite_index_test.rs"],
    },
    TicketContract {
        id: 13,
        disposition: Disposition::ExternalLifecycle,
        owners: &["tests/cold_composite_index_test.rs"],
    },
    TicketContract {
        id: 14,
        disposition: Disposition::Regression,
        owners: &["tests/timestamp_index_plan_test.rs"],
    },
    TicketContract {
        id: 15,
        disposition: Disposition::Regression,
        owners: &["tests/multi_index_or_plan_test.rs"],
    },
    TicketContract {
        id: 16,
        disposition: Disposition::Regression,
        owners: &[
            "tests/bound_uuid_in_test.rs",
            "tests/cold_composite_index_test.rs",
        ],
    },
    TicketContract {
        id: 17,
        disposition: Disposition::Regression,
        owners: &["tests/cold_indexed_join_test.rs"],
    },
    TicketContract {
        id: 18,
        disposition: Disposition::ExternalLifecycle,
        owners: &[
            "tests/transaction_index_visibility_test.rs",
            "tests/prerelease_concurrency_test.rs",
        ],
    },
    TicketContract {
        id: 19,
        disposition: Disposition::Regression,
        owners: &[
            "crates/radixdb-client/src/async_client.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
    TicketContract {
        id: 20,
        disposition: Disposition::Regression,
        owners: &[
            "tests/ddl_transaction_test.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
    TicketContract {
        id: 21,
        disposition: Disposition::DeferredPerformance,
        owners: &["doc/src/content/docs/en/appendices/benchmarks.md"],
    },
    TicketContract {
        id: 22,
        disposition: Disposition::Regression,
        owners: &["tests/rdb_0022_check_lifecycle_test.rs"],
    },
    TicketContract {
        id: 23,
        disposition: Disposition::Regression,
        owners: &["tests/bound_uuid_in_test.rs"],
    },
    TicketContract {
        id: 24,
        disposition: Disposition::Regression,
        owners: &["tests/prerelease_ticket_belt_test.rs"],
    },
    TicketContract {
        id: 25,
        disposition: Disposition::Regression,
        owners: &["tests/prerelease_ticket_belt_test.rs"],
    },
    TicketContract {
        id: 26,
        disposition: Disposition::Regression,
        owners: &["tests/rdb_0026_alter_foreign_key_test.rs"],
    },
    TicketContract {
        id: 27,
        disposition: Disposition::Regression,
        owners: &["tests/rdb_0027_forwarded_attachment_join_test.rs"],
    },
    TicketContract {
        id: 28,
        disposition: Disposition::Regression,
        owners: &[
            "src/server/tcp_server.rs",
            "tests/prerelease_concurrency_test.rs",
        ],
    },
    TicketContract {
        id: 29,
        disposition: Disposition::OpenExternalAcceptance,
        owners: &[
            "tests/prerelease_ticket_belt_test.rs",
            "tests/transaction_index_visibility_test.rs",
        ],
    },
    TicketContract {
        id: 30,
        disposition: Disposition::Regression,
        owners: &[
            "tests/rdb_0030_transactional_index_publication_test.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
    TicketContract {
        id: 31,
        disposition: Disposition::Regression,
        owners: &[
            "tests/rdb_0031_fk_parent_non_key_update_test.rs",
            "tests/tcp_ddl_transaction_test.rs",
        ],
    },
];

fn affected(connection: &mut Connection, sql: impl Into<String>) -> Result<u64, String> {
    match connection.execute(sql).map_err(|error| error.to_string())? {
        ExecuteResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
        ExecuteResult::Cursor(cursor) => {
            connection
                .close_cursor(cursor)
                .map_err(|error| error.to_string())?;
            Err("DML unexpectedly returned a cursor".to_string())
        }
    }
}

#[test]
fn b9_ticket_inventory_is_complete_classified_and_repo_local() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let ids = TICKETS
        .iter()
        .map(|ticket| ticket.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids, (1..=31).collect(), "ticket inventory must be gap-free");
    assert_eq!(
        TICKETS
            .iter()
            .filter(|ticket| ticket.disposition == Disposition::DeferredPerformance)
            .map(|ticket| ticket.id)
            .collect::<Vec<_>>(),
        vec![21]
    );
    assert_eq!(
        TICKETS
            .iter()
            .filter(|ticket| ticket.disposition == Disposition::OpenExternalAcceptance)
            .map(|ticket| ticket.id)
            .collect::<Vec<_>>(),
        vec![29]
    );
    for ticket in TICKETS {
        assert!(
            !ticket.owners.is_empty(),
            "RDB-{:04} has no owner",
            ticket.id
        );
        for owner in ticket.owners {
            assert!(!owner.starts_with('/'));
            assert!(!owner.contains("mozaic-im"));
            assert!(
                root.join(owner).is_file(),
                "RDB-{:04} owner does not exist: {owner}",
                ticket.id
            );
        }
    }
}

#[test]
fn b9_oracle_treats_commit_as_publication_not_statement_success() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b9-publication-").unwrap();
    let data = fixture.child("server-data").unwrap();
    with_tcp_server(data, 8, |address| {
        let mut owner = tcp_connect(address, "b9_publication").unwrap();
        tcp_command(
            &mut owner,
            "CREATE TABLE publication_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL UNIQUE)",
        )
        .unwrap();
        tcp_command(&mut owner, "INSERT INTO publication_probe VALUES (1, 10)").unwrap();
        let mut observer = tcp_connect(address, "b9_publication").unwrap();

        owner.begin().unwrap();
        assert_eq!(
            affected(
                &mut owner,
                "UPDATE publication_probe SET value = 20 WHERE id = 1",
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT COUNT(*) FROM publication_probe WHERE value = 20",
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut observer,
                "SELECT COUNT(*) FROM publication_probe WHERE value = 10",
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut observer,
                "SELECT COUNT(*) FROM publication_probe WHERE value = 20",
            )
            .unwrap(),
            0
        );
        owner.rollback().unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut observer,
                "SELECT value FROM publication_probe WHERE id = 1",
            )
            .unwrap(),
            10
        );
    });
}

#[test]
fn b9_rdb_0024_refresh_token_index_rotation_is_exact_after_reopen() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b9-rdb0024-").unwrap();
    let data = fixture.child("server-data").unwrap();
    with_tcp_server(data.clone(), 16, |address| {
        let mut connection = tcp_connect(address, DATABASE).unwrap();
        for sql in [
            "CREATE TABLE sessions (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, refresh_token_hash TEXT NOT NULL UNIQUE, revision INTEGER NOT NULL)",
            "CREATE TABLE used_refresh_tokens (id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES sessions(id), token_hash TEXT NOT NULL UNIQUE)",
            "INSERT INTO sessions VALUES (1, 7, 'old-token', 0)",
        ] {
            tcp_command(&mut connection, sql).unwrap();
        }
        connection.begin().unwrap();
        assert_eq!(
            affected(
                &mut connection,
                "INSERT INTO used_refresh_tokens VALUES (1, 1, 'old-token')"
            )
            .unwrap(),
            1
        );
        assert_eq!(
            affected(
                &mut connection,
                "UPDATE sessions SET refresh_token_hash = 'new-token', revision = revision + 1 WHERE id = 1 AND revision = 0 AND refresh_token_hash = 'old-token'"
            )
            .unwrap(),
            1
        );
        connection.commit().unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM sessions WHERE refresh_token_hash = 'old-token'"
            )
            .unwrap(),
            0
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM sessions WHERE refresh_token_hash = 'new-token'"
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM used_refresh_tokens WHERE token_hash = 'old-token'"
            )
            .unwrap(),
            1
        );
        tcp_command(&mut connection, "PRAGMA CHECKPOINT").unwrap();
    });
    with_tcp_server(data, 16, |address| {
        let mut connection = tcp_connect(address, DATABASE).unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM sessions WHERE refresh_token_hash = 'old-token'"
            )
            .unwrap(),
            0
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM sessions WHERE refresh_token_hash = 'new-token'"
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM used_refresh_tokens WHERE token_hash = 'old-token'"
            )
            .unwrap(),
            1
        );
    });
}

#[test]
fn b9_rdb_0025_outbox_lease_wave_survives_preceding_tcp_workload() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b9-rdb0025-").unwrap();
    let data = fixture.child("server-data").unwrap();
    with_tcp_server(data, 32, |address| {
        let mut owner = tcp_connect(address, DATABASE).unwrap();
        for sql in [
            "CREATE TABLE preload (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            "CREATE INDEX preload_value_idx ON preload(value)",
            "CREATE VIEW preload_v AS SELECT value, COUNT(*) AS n FROM preload GROUP BY value",
            "CREATE TABLE outbox_jobs (id INTEGER PRIMARY KEY, state TEXT NOT NULL, visible_at INTEGER NOT NULL, lease_until INTEGER NOT NULL, lease_owner TEXT)",
            "CREATE INDEX outbox_lease_idx ON outbox_jobs(state, visible_at, lease_until)",
            "CREATE TABLE claim_audit (job_id INTEGER PRIMARY KEY REFERENCES outbox_jobs(id), worker INTEGER NOT NULL)",
        ] {
            tcp_command(&mut owner, sql).unwrap();
        }
        for id in 1..=256 {
            tcp_command(
                &mut owner,
                format!("INSERT INTO preload VALUES ({id}, {})", id % 17),
            )
            .unwrap();
        }
        for id in 1..=32 {
            tcp_command(
                &mut owner,
                format!("INSERT INTO outbox_jobs VALUES ({id}, 'pending', 0, 0, NULL)"),
            )
            .unwrap();
        }
        for sql in [
            "INSERT INTO outbox_jobs VALUES (101, 'leased', 0, 9999, 'active')",
            "INSERT INTO outbox_jobs VALUES (102, 'pending', 9999, 0, NULL)",
            "INSERT INTO outbox_jobs VALUES (103, 'done', 0, 0, NULL)",
            "SELECT COUNT(*) FROM preload_v",
            "SELECT COUNT(*) FROM preload WHERE value IN (1, 2, 3)",
            "PRAGMA CHECKPOINT",
        ] {
            tcp_command(&mut owner, sql).unwrap();
        }

        let barrier = Arc::new(Barrier::new(8));
        let workers = (0..8i64)
            .map(|worker| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || -> Result<u64, String> {
                    let mut connection = tcp_connect(address, DATABASE)?;
                    barrier.wait();
                    let mut claims = 0;
                    for id in 1..=32i64 {
                        let mut completed = false;
                        for _ in 0..32 {
                            connection.begin().map_err(|error| error.to_string())?;
                            match affected(
                                &mut connection,
                                format!(
                                    "UPDATE outbox_jobs SET state = 'leased', lease_until = 100, lease_owner = 'worker-{worker}' WHERE id = {id} AND state = 'pending' AND visible_at <= 0 AND lease_until <= 0"
                                ),
                            ) {
                                Ok(1) => {
                                    affected(
                                        &mut connection,
                                        format!("INSERT INTO claim_audit VALUES ({id}, {worker})"),
                                    )?;
                                    connection.commit().map_err(|error| error.to_string())?;
                                    claims += 1;
                                    completed = true;
                                    break;
                                }
                                Ok(0) => {
                                    connection.rollback().map_err(|error| error.to_string())?;
                                    completed = true;
                                    break;
                                }
                                Ok(other) => return Err(format!("claim updated {other} rows")),
                                Err(_) => {
                                    let _ = connection.rollback();
                                    thread::yield_now();
                                }
                            }
                        }
                        if !completed {
                            return Err(format!("worker {worker} exhausted retries for job {id}"));
                        }
                    }
                    Ok(claims)
                })
            })
            .collect::<Vec<_>>();
        let claimed = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .sum::<u64>();
        assert_eq!(claimed, 32);
        assert_eq!(
            tcp_scalar_i64(&mut owner, "SELECT COUNT(*) FROM claim_audit").unwrap(),
            32
        );
        assert_eq!(tcp_scalar_i64(&mut owner, "SELECT COUNT(*) FROM outbox_jobs WHERE id <= 32 AND state = 'leased' AND lease_owner IS NOT NULL").unwrap(), 32);
        assert_eq!(tcp_scalar_i64(&mut owner, "SELECT COUNT(*) FROM outbox_jobs WHERE id = 101 AND state = 'leased' AND lease_owner = 'active'").unwrap(), 1);
        assert_eq!(tcp_scalar_i64(&mut owner, "SELECT COUNT(*) FROM outbox_jobs WHERE id = 102 AND state = 'pending' AND lease_owner IS NULL").unwrap(), 1);
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT COUNT(*) FROM outbox_jobs WHERE id = 103 AND state = 'done'"
            )
            .unwrap(),
            1
        );
        assert_eq!(affected(&mut owner, "UPDATE outbox_jobs SET state = 'done' WHERE id = 1 AND lease_owner = 'stale-owner'").unwrap(), 0);
    });
}

fn embedded_unique_swap(db: &Database) {
    let mut transaction = db.begin().unwrap();
    transaction
        .execute(
            "UPDATE endpoints SET position = 10 WHERE id = '00000000-0000-0000-0000-000000000009'",
            (),
        )
        .unwrap();
    assert_eq!(
        transaction
            .query_one::<i64, _>("SELECT COUNT(*) FROM endpoints WHERE position = 9", ())
            .unwrap(),
        0
    );

    for (id, position) in [
        ("00000000-0000-0000-0000-000000000006", 9),
        ("00000000-0000-0000-0000-000000000007", 10),
    ] {
        let mut contender = db.begin().unwrap();
        let insert = format!(
            "INSERT INTO endpoints VALUES ('{id}', \
             '10000000-0000-0000-0000-000000000001', {position})"
        );
        assert!(
            contender.execute(&insert, ()).is_err(),
            "a concurrent transaction must not acquire position {position}"
        );
        contender.rollback().unwrap();
    }

    transaction
        .execute(
            "UPDATE endpoints SET position = 9 WHERE id = '00000000-0000-0000-0000-000000000008'",
            (),
        )
        .unwrap();
    transaction
        .execute(
            "UPDATE endpoints SET position = 8 WHERE id = '00000000-0000-0000-0000-000000000009'",
            (),
        )
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn b9_rdb_0029_composite_unique_key_is_reusable_within_one_transaction() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b9-rdb0029-").unwrap();
    let path = fixture.child("database").unwrap();
    let dsn = format!("file://{}?checkpoint_on_close=off", path.display());
    let db = Database::open(&dsn).unwrap();
    for sql in [
        "CREATE TABLE profiles (id UUID PRIMARY KEY)",
        "CREATE TABLE endpoints (id UUID PRIMARY KEY, profile_id UUID NOT NULL REFERENCES profiles(id), position INTEGER NOT NULL CHECK (position >= 1), UNIQUE (profile_id, position))",
        "INSERT INTO profiles VALUES ('10000000-0000-0000-0000-000000000001')",
        "INSERT INTO endpoints VALUES ('00000000-0000-0000-0000-000000000008', '10000000-0000-0000-0000-000000000001', 8)",
        "INSERT INTO endpoints VALUES ('00000000-0000-0000-0000-000000000009', '10000000-0000-0000-0000-000000000001', 9)",
    ] {
        db.execute(sql, ()).unwrap();
    }
    for statements in 1..=3 {
        let mut transaction = db.begin().unwrap();
        let updates = [
            "UPDATE endpoints SET position = 10 WHERE id = '00000000-0000-0000-0000-000000000009'",
            "UPDATE endpoints SET position = 9 WHERE id = '00000000-0000-0000-0000-000000000008'",
            "UPDATE endpoints SET position = 8 WHERE id = '00000000-0000-0000-0000-000000000009'",
        ];
        for update in updates.into_iter().take(statements) {
            transaction.execute(update, ()).unwrap();
        }
        transaction.rollback().unwrap();
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT COUNT(*) FROM endpoints WHERE position IN (8, 9)",
                ()
            )
            .unwrap(),
            2
        );
    }
    embedded_unique_swap(&db);
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT position FROM endpoints WHERE id = '00000000-0000-0000-0000-000000000008'",
            ()
        )
        .unwrap(),
        9
    );
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT position FROM endpoints WHERE id = '00000000-0000-0000-0000-000000000009'",
            ()
        )
        .unwrap(),
        8
    );
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    db.close().unwrap();

    let db = Database::open(&dsn).unwrap();
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT COUNT(*) FROM endpoints WHERE position IN (8, 9)",
            ()
        )
        .unwrap(),
        2
    );
    db.close().unwrap();

    let server_data = fixture.child("server-data").unwrap();
    with_tcp_server(server_data.clone(), 16, |address| {
        let mut owner = tcp_connect(address, "rdb0029_tcp").unwrap();
        for sql in [
            "CREATE TABLE profiles (id UUID PRIMARY KEY)",
            "CREATE TABLE endpoints (id UUID PRIMARY KEY, profile_id UUID NOT NULL REFERENCES profiles(id), position INTEGER NOT NULL CHECK (position >= 1), UNIQUE (profile_id, position))",
            "INSERT INTO profiles VALUES ('10000000-0000-0000-0000-000000000001')",
            "INSERT INTO endpoints VALUES ('00000000-0000-0000-0000-000000000008', '10000000-0000-0000-0000-000000000001', 8)",
            "INSERT INTO endpoints VALUES ('00000000-0000-0000-0000-000000000009', '10000000-0000-0000-0000-000000000001', 9)",
            "PRAGMA CHECKPOINT",
        ] {
            tcp_command(&mut owner, sql).unwrap();
        }

        owner.begin().unwrap();
        assert_eq!(
            affected(
                &mut owner,
                "UPDATE endpoints SET position = 10 WHERE id = '00000000-0000-0000-0000-000000000009'",
            )
            .unwrap(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT COUNT(*) FROM endpoints WHERE position = 9",
            )
            .unwrap(),
            0
        );

        for sql in [
            "UPDATE endpoints SET position = 9 WHERE id = '00000000-0000-0000-0000-000000000008'",
            "UPDATE endpoints SET position = 8 WHERE id = '00000000-0000-0000-0000-000000000009'",
        ] {
            assert_eq!(affected(&mut owner, sql).unwrap(), 1);
        }
        owner.commit().unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT position FROM endpoints WHERE id = '00000000-0000-0000-0000-000000000008'",
            )
            .unwrap(),
            9
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT position FROM endpoints WHERE id = '00000000-0000-0000-0000-000000000009'",
            )
            .unwrap(),
            8
        );
    });
    with_tcp_server(server_data, 16, |address| {
        let mut connection = tcp_connect(address, "rdb0029_tcp").unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM endpoints WHERE position IN (8, 9)",
            )
            .unwrap(),
            2
        );
    });
}
