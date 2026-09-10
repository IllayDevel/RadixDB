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
use radixdb_join_workload::{
    apply_schema, execute_case, seed_database, verify_fixture_manifest, CaseId, WorkloadScale,
};

#[test]
fn frozen_fixture_manifest_is_complete_and_valid() {
    let manifest = verify_fixture_manifest().expect("fixture manifest must be valid");
    assert_eq!(manifest.fixtures.len(), 8);
}

#[test]
fn smoke_profile_executes_the_complete_consumer_corpus() {
    const ORACLE: [(CaseId, usize, &str); 7] = [
        (
            CaseId::Q1MessagePush,
            2,
            "80a76939ea26fb61bebb6e58d532aabba6f1b2ccb8440265555a8bf60ca93412",
        ),
        (
            CaseId::Q2CallPush,
            2,
            "515ac689749ec9e799551c0276cfcc4f7c1e6fc7f1af27067edceb0aef5ccd0e",
        ),
        (
            CaseId::Q3StreamPush,
            2,
            "8f03d44e53e80e196a04e120280bb27097920c105e22bc68192ae9a31e22e003",
        ),
        (
            CaseId::Q4PublicationRecipients,
            8,
            "50e8b1e82e283a9172ffc9e8ff22bee0d538ffa6a1cd146f778381740dde7817",
        ),
        (
            CaseId::Q5SnapshotUsers,
            32,
            "696a4173a1721becfb5ba6a0476ea9d169e7c87cab4708be22dfe08a3fb41b6f",
        ),
        (
            CaseId::Q6SnapshotMembers,
            32,
            "5daa0eebd524db6369e4e2fe7db371b69234b06c926af46a9882526d40f5d611",
        ),
        (
            CaseId::C1OutboxPoll,
            4,
            "ddcf3b42ded905ac99c2bbe1ec2174dfe7ca75d198705a0f10a72649d734a460",
        ),
    ];

    let scale = WorkloadScale::smoke();
    let db = Database::open_in_memory().expect("open in-memory database");
    apply_schema(&db).expect("apply frozen schema");
    seed_database(&db, scale).expect("seed deterministic workload");

    for (case, oracle_rows, oracle_checksum) in ORACLE {
        let first = execute_case(&db, case, scale).unwrap_or_else(|error| {
            panic!("{} failed: {error}", case.name());
        });
        let second = execute_case(&db, case, scale).unwrap_or_else(|error| {
            panic!("{} repeat failed: {error}", case.name());
        });
        println!(
            "{} rows={} checksum={}",
            case.name(),
            first.rows,
            first.checksum_sha256
        );
        assert_eq!(first.rows, scale.expected_rows(case), "{}", case.name());
        assert_eq!(first.rows, oracle_rows, "{}", case.name());
        assert_eq!(first.checksum_sha256, oracle_checksum, "{}", case.name());
        assert_eq!(
            first.checksum_sha256,
            second.checksum_sha256,
            "{}",
            case.name()
        );
    }
}
