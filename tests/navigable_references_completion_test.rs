// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Completion evidence for the mandatory schemas and the full NR benchmark matrix.

use std::{hint::black_box, time::Instant};

use radixdb::{Database, Value};

struct MatrixCase {
    name: &'static str,
    navigation: &'static str,
    explicit: &'static str,
}

const MATRIX: &[MatrixCase] = &[
    MatrixCase {
        name: "reference.single_pk",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_roots r WHERE r.id = 1",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   WHERE r.id = 1",
    },
    MatrixCase {
        name: "reference.batch_repeated",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_roots r ORDER BY r.id",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.batch_unique",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_unique_roots r ORDER BY r.id",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_unique_roots r
                   LEFT JOIN nr_matrix_unique_targets t ON r.target_id = t.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.multifield",
        navigation: "SELECT r.id, r.target_id.label, r.target_id.code
                     FROM nr_matrix_roots r ORDER BY r.id",
        explicit: "SELECT r.id, t.label, t.code
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.transitive_2",
        navigation: "SELECT r.id, r.target_id.region_id.label
                     FROM nr_matrix_roots r ORDER BY r.id",
        explicit: "SELECT r.id, g.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   LEFT JOIN nr_matrix_regions g ON t.region_id = g.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.transitive_4",
        navigation: "SELECT r.id, r.l1_id.l2_id.l3_id.l4_id.label
                     FROM nr_matrix_deep_roots r ORDER BY r.id",
        explicit: "SELECT r.id, l4.label
                   FROM nr_matrix_deep_roots r
                   LEFT JOIN nr_matrix_l1 l1 ON r.l1_id = l1.id
                   LEFT JOIN nr_matrix_l2 l2 ON l1.l2_id = l2.id
                   LEFT JOIN nr_matrix_l3 l3 ON l2.l3_id = l3.id
                   LEFT JOIN nr_matrix_l4 l4 ON l3.l4_id = l4.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.nullable",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_roots r
                     WHERE r.target_id IS NULL OR r.id <= 1024
                     ORDER BY r.id",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   WHERE r.target_id IS NULL OR r.id <= 1024
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.target_filter",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_roots r
                     WHERE r.target_id.label LIKE 'target-1%'
                     ORDER BY r.id",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   WHERE t.label LIKE 'target-1%'
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.is_null",
        navigation: "SELECT r.id
                     FROM nr_matrix_roots r
                     WHERE r.target_id.label IS NULL
                     ORDER BY r.id",
        explicit: "SELECT r.id
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   WHERE t.label IS NULL
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.hot_cold",
        navigation: "SELECT r.id, r.target_id.label
                     FROM nr_matrix_roots r ORDER BY r.id",
        explicit: "SELECT r.id, t.label
                   FROM nr_matrix_roots r
                   LEFT JOIN nr_matrix_targets t ON r.target_id = t.id
                   ORDER BY r.id",
    },
    MatrixCase {
        name: "reference.fact_dimension",
        navigation: "SELECT p.target_id.region_id.label, SUM(p.amount)
                     FROM nr_matrix_payments p
                     GROUP BY p.target_id.region_id.label
                     ORDER BY p.target_id.region_id.label",
        explicit: "SELECT g.label, SUM(p.amount)
                   FROM nr_matrix_payments p
                   LEFT JOIN nr_matrix_targets t ON p.target_id = t.id
                   LEFT JOIN nr_matrix_regions g ON t.region_id = g.id
                   GROUP BY g.label
                   ORDER BY g.label",
    },
];

fn values(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error}"))
        .map(|row| row.expect("query row").into_inner().into_values())
        .collect()
}

fn insert_values(db: &Database, table: &str, rows: impl IntoIterator<Item = String>) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    for chunk in rows.chunks(256) {
        db.execute(
            &format!("INSERT INTO {table} VALUES {}", chunk.join(",")),
            (),
        )
        .unwrap_or_else(|error| panic!("insert into {table} failed: {error}"));
    }
}

fn setup_matrix(db: &Database, root_rows: usize) {
    for sql in [
        "CREATE TABLE nr_matrix_regions (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )",
        "CREATE TABLE nr_matrix_targets (
            id INTEGER PRIMARY KEY,
            region_id INTEGER NOT NULL REFERENCES nr_matrix_regions(id),
            label TEXT NOT NULL,
            code TEXT NOT NULL
        )",
        "CREATE TABLE nr_matrix_roots (
            id INTEGER PRIMARY KEY,
            target_id INTEGER REFERENCES nr_matrix_targets(id),
            payload INTEGER NOT NULL
        )",
        "CREATE TABLE nr_matrix_unique_targets (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )",
        "CREATE TABLE nr_matrix_unique_roots (
            id INTEGER PRIMARY KEY,
            target_id INTEGER NOT NULL REFERENCES nr_matrix_unique_targets(id)
        )",
        "CREATE TABLE nr_matrix_l4 (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        "CREATE TABLE nr_matrix_l3 (
            id INTEGER PRIMARY KEY,
            l4_id INTEGER REFERENCES nr_matrix_l4(id)
        )",
        "CREATE TABLE nr_matrix_l2 (
            id INTEGER PRIMARY KEY,
            l3_id INTEGER REFERENCES nr_matrix_l3(id)
        )",
        "CREATE TABLE nr_matrix_l1 (
            id INTEGER PRIMARY KEY,
            l2_id INTEGER REFERENCES nr_matrix_l2(id)
        )",
        "CREATE TABLE nr_matrix_deep_roots (
            id INTEGER PRIMARY KEY,
            l1_id INTEGER REFERENCES nr_matrix_l1(id)
        )",
        "CREATE TABLE nr_matrix_payments (
            id INTEGER PRIMARY KEY,
            target_id INTEGER NOT NULL REFERENCES nr_matrix_targets(id),
            amount INTEGER NOT NULL
        )",
    ] {
        db.execute(sql, ()).unwrap();
    }

    insert_values(
        db,
        "nr_matrix_regions",
        (1..=64).map(|id| format!("({id}, 'region-{id}')")),
    );
    insert_values(
        db,
        "nr_matrix_targets",
        (1..=256).map(|id| {
            format!(
                "({id}, {}, 'target-{id}', 'code-{id}')",
                ((id - 1) % 64) + 1
            )
        }),
    );
    insert_values(
        db,
        "nr_matrix_roots",
        (1..=root_rows).map(|id| {
            if id % 5 == 0 {
                format!("({id}, NULL, {id})")
            } else {
                format!("({id}, {}, {id})", ((id - 1) % 256) + 1)
            }
        }),
    );
    insert_values(
        db,
        "nr_matrix_unique_targets",
        (1..=64).map(|id| format!("({id}, 'unique-{id}')")),
    );
    insert_values(
        db,
        "nr_matrix_unique_roots",
        (1..=64).map(|id| format!("({id}, {id})")),
    );
    insert_values(
        db,
        "nr_matrix_l4",
        (1..=32).map(|id| format!("({id}, 'deep-{id}')")),
    );
    insert_values(
        db,
        "nr_matrix_l3",
        (1..=32).map(|id| format!("({id}, {id})")),
    );
    insert_values(
        db,
        "nr_matrix_l2",
        (1..=32).map(|id| format!("({id}, {id})")),
    );
    insert_values(
        db,
        "nr_matrix_l1",
        (1..=32).map(|id| format!("({id}, {id})")),
    );
    insert_values(
        db,
        "nr_matrix_deep_roots",
        (1..=128).map(|id| format!("({id}, {})", ((id - 1) % 32) + 1)),
    );
    insert_values(
        db,
        "nr_matrix_payments",
        (1..=root_rows)
            .map(|id| format!("({id}, {}, {})", ((id - 1) % 256) + 1, (id % 10_000) + 1)),
    );
}

fn assert_matrix(db: &Database, phase: &str) {
    for case in MATRIX {
        assert_eq!(
            values(db, case.navigation),
            values(db, case.explicit),
            "{} parity in {phase}",
            case.name
        );
    }
}

#[test]
fn mandatory_uuid_single_transitive_and_self_reference_schemas_match_left_joins() {
    let db = Database::open("memory://navigation_completion_uuid").unwrap();
    for sql in [
        "CREATE TABLE referenced_values (
            id UUID PRIMARY KEY,
            scalar TEXT,
            optional_scalar TEXT
        )",
        "CREATE TABLE roots (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            referenced_value_id UUID REFERENCES referenced_values(id)
        )",
        "CREATE TABLE profiles (
            id UUID PRIMARY KEY,
            display_name TEXT NOT NULL
        )",
        "CREATE TABLE users (
            id UUID PRIMARY KEY,
            profile_id UUID REFERENCES profiles(id)
        )",
        "CREATE TABLE messages (
            id UUID PRIMARY KEY,
            sender_id UUID REFERENCES users(id),
            body TEXT NOT NULL
        )",
        "CREATE TABLE employees (
            id UUID PRIMARY KEY,
            manager_id UUID REFERENCES employees(id),
            name TEXT NOT NULL
        )",
        "INSERT INTO referenced_values VALUES
            ('00000000-0000-0000-0000-000000000001', 'one', NULL),
            ('00000000-0000-0000-0000-000000000002', 'two', 'optional')",
        "INSERT INTO roots VALUES
            ('10000000-0000-0000-0000-000000000001', 'root-one',
             '00000000-0000-0000-0000-000000000001'),
            ('10000000-0000-0000-0000-000000000002', 'root-null', NULL),
            ('10000000-0000-0000-0000-000000000003', 'root-two',
             '00000000-0000-0000-0000-000000000002')",
        "INSERT INTO profiles VALUES
            ('20000000-0000-0000-0000-000000000001', 'Alice'),
            ('20000000-0000-0000-0000-000000000002', 'Bob')",
        "INSERT INTO users VALUES
            ('30000000-0000-0000-0000-000000000001',
             '20000000-0000-0000-0000-000000000001'),
            ('30000000-0000-0000-0000-000000000002', NULL)",
        "INSERT INTO messages VALUES
            ('40000000-0000-0000-0000-000000000001',
             '30000000-0000-0000-0000-000000000001', 'from-alice'),
            ('40000000-0000-0000-0000-000000000002',
             '30000000-0000-0000-0000-000000000002', 'without-profile'),
            ('40000000-0000-0000-0000-000000000003', NULL, 'without-sender')",
        "INSERT INTO employees VALUES
            ('50000000-0000-0000-0000-000000000001', NULL, 'CEO'),
            ('50000000-0000-0000-0000-000000000002',
             '50000000-0000-0000-0000-000000000001', 'Manager'),
            ('50000000-0000-0000-0000-000000000003',
             '50000000-0000-0000-0000-000000000002', 'Worker')",
    ] {
        db.execute(sql, ())
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    for (navigation, explicit) in [
        (
            "SELECT r.name, r.referenced_value_id.scalar,
                    r.referenced_value_id.optional_scalar
             FROM roots r ORDER BY r.name",
            "SELECT r.name, v.scalar, v.optional_scalar
             FROM roots r
             LEFT JOIN referenced_values v ON r.referenced_value_id = v.id
             ORDER BY r.name",
        ),
        (
            "SELECT m.body, m.sender_id.profile_id.display_name
             FROM messages m ORDER BY m.body",
            "SELECT m.body, p.display_name
             FROM messages m
             LEFT JOIN users u ON m.sender_id = u.id
             LEFT JOIN profiles p ON u.profile_id = p.id
             ORDER BY m.body",
        ),
        (
            "SELECT e.name, e.manager_id.manager_id.name
             FROM employees e ORDER BY e.name",
            "SELECT e.name, senior.name
             FROM employees e
             LEFT JOIN employees manager ON e.manager_id = manager.id
             LEFT JOIN employees senior ON manager.manager_id = senior.id
             ORDER BY e.name",
        ),
    ] {
        assert_eq!(values(&db, navigation), values(&db, explicit));
    }
}

#[test]
fn minimum_benchmark_matrix_has_executable_join_parity_and_storage_modes() {
    let db = Database::open("memory://navigation_completion_matrix").unwrap();
    setup_matrix(&db, 4_096);
    assert_matrix(&db, "hot");

    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/navigation_completion_matrix?sync_mode=normal&checkpoint_interval=3600&checkpoint_on_close=off",
        directory.path().display()
    );
    {
        let file_db = Database::open(&dsn).unwrap();
        setup_matrix(&file_db, 1_024);
        assert_matrix(&file_db, "file-hot");
        file_db.execute("PRAGMA snapshot", ()).unwrap();
        assert_matrix(&file_db, "cold");
        file_db
            .execute(
                "INSERT INTO nr_matrix_targets VALUES
                    (1000, 1, 'target-overlay', 'code-overlay')",
                (),
            )
            .unwrap();
        file_db
            .execute(
                "INSERT INTO nr_matrix_roots VALUES (10000, 1000, 10000)",
                (),
            )
            .unwrap();
        assert_matrix(&file_db, "hybrid");
    }
    let reopened = Database::open(&dsn).unwrap();
    assert_matrix(&reopened, "reopen");
}

fn median_ms(db: &Database, sql: &str) -> f64 {
    black_box(values(db, sql));
    let mut samples = (0..5)
        .map(|_| {
            let started = Instant::now();
            black_box(values(db, sql));
            started.elapsed().as_secs_f64() * 1000.0
        })
        .collect::<Vec<_>>();
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[test]
#[ignore = "manual NR completion release benchmark; run with --release --ignored --nocapture"]
fn release_minimum_benchmark_matrix_reports_navigation_and_explicit_medians() {
    let db = Database::open("memory://navigation_completion_release_matrix").unwrap();
    setup_matrix(&db, 100_000);

    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/navigation_completion_release_hybrid?sync_mode=normal&checkpoint_interval=3600&checkpoint_on_close=off",
        directory.path().display()
    );
    let hybrid = Database::open(&dsn).unwrap();
    setup_matrix(&hybrid, 100_000);
    hybrid.execute("PRAGMA snapshot", ()).unwrap();
    hybrid
        .execute(
            "INSERT INTO nr_matrix_targets VALUES
                (1000, 1, 'target-overlay', 'code-overlay')",
            (),
        )
        .unwrap();
    hybrid
        .execute(
            "INSERT INTO nr_matrix_roots VALUES (100001, 1000, 100001)",
            (),
        )
        .unwrap();

    eprintln!("| Case | Navigation, ms | Explicit JOIN, ms | Ratio |");
    eprintln!("|---|---:|---:|---:|");
    for case in MATRIX {
        let case_db = if case.name == "reference.hot_cold" {
            &hybrid
        } else {
            &db
        };
        assert_eq!(
            values(case_db, case.navigation),
            values(case_db, case.explicit)
        );
        let navigation_ms = median_ms(case_db, case.navigation);
        let explicit_ms = median_ms(case_db, case.explicit);
        eprintln!(
            "| `{}` | {:.3} | {:.3} | {:.3}x |",
            case.name,
            navigation_ms,
            explicit_ms,
            navigation_ms / explicit_ms
        );
    }
}
