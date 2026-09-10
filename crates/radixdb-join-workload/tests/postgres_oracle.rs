// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0

#![cfg(feature = "postgres-oracle")]

use postgres::{Client, NoTls};
use radixdb::Database;
use radixdb_join_workload::postgres_oracle;
use radixdb_join_workload::{apply_schema, execute_case, seed_database, CaseId, WorkloadScale};

#[test]
#[ignore = "requires the registered dedicated PostgreSQL 18 benchmark cluster"]
fn q1_q6_and_control_match_postgresql_rows_order_types_and_nulls() {
    let dsn = std::env::var("RADIXDB_JOIN_PG_DSN")
        .expect("RADIXDB_JOIN_PG_DSN must identify the registered benchmark cluster");
    let mut postgres = Client::connect(&dsn, NoTls).expect("connect PostgreSQL oracle");
    let server_version = postgres
        .query_one("SHOW server_version_num", &[])
        .expect("read PostgreSQL version")
        .get::<_, String>(0);
    assert!(
        server_version.starts_with("18"),
        "PostgreSQL 18 is required, got {server_version}"
    );

    let mut transaction = postgres.transaction().expect("begin PostgreSQL oracle");
    transaction
        .batch_execute(
            "CREATE SCHEMA jr10_join_oracle;
             SET LOCAL search_path TO jr10_join_oracle, public;",
        )
        .expect("create isolated oracle schema");

    let scale = WorkloadScale::smoke();
    postgres_oracle::apply_schema(&mut transaction).expect("apply PostgreSQL schema");
    postgres_oracle::seed_database(&mut transaction, scale).expect("seed PostgreSQL corpus");

    let radix = Database::open_in_memory().expect("open RadixDB oracle");
    apply_schema(&radix).expect("apply RadixDB schema");
    seed_database(&radix, scale).expect("seed RadixDB corpus");

    for case in CaseId::ALL {
        let expected = execute_case(&radix, case, scale)
            .unwrap_or_else(|error| panic!("{} RadixDB failed: {error}", case.name()));
        let actual = postgres_oracle::execute_case(&mut transaction, case, scale)
            .unwrap_or_else(|error| panic!("{} PostgreSQL failed: {error:?}", case.name()));
        assert_eq!(actual.rows, expected.rows, "{} rows", case.name());
        assert_eq!(
            actual.canonical_result_bytes,
            expected.canonical_result_bytes,
            "{} canonical bytes",
            case.name()
        );
        assert_eq!(
            actual.checksum_sha256,
            expected.checksum_sha256,
            "{} checksum",
            case.name()
        );
    }

    transaction
        .rollback()
        .expect("rollback isolated oracle schema");
}
