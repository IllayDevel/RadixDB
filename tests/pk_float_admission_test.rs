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

//! AUD-CONTRACT-205 regression oracles for Float admission to INTEGER PK paths.

use radixdb::{named_params, Database};

fn query_ids<P: radixdb::Params>(db: &Database, sql: &str, params: P) -> Vec<i64> {
    db.query(sql, params)
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

fn setup(name: &str) -> Database {
    let db = Database::open(&format!("memory://{name}")).unwrap();
    db.execute(
        "CREATE TABLE pk_float_rows (id INTEGER PRIMARY KEY, payload TEXT)",
        (),
    )
    .unwrap();
    for id in 1_i64..=13 {
        db.execute(
            "INSERT INTO pk_float_rows VALUES ($1, $2)",
            (id, format!("row-{id}")),
        )
        .unwrap();
    }
    db.execute("INSERT INTO pk_float_rows VALUES ($1, 'max')", (i64::MAX,))
        .unwrap();
    db
}

#[test]
fn select_pk_fast_path_revalidates_literal_and_parameter_values() {
    let db = setup("pk_float_select");

    assert_eq!(
        query_ids(&db, "SELECT * FROM pk_float_rows WHERE id = 2.0", (),),
        vec![2]
    );
    assert!(query_ids(&db, "SELECT * FROM pk_float_rows WHERE id = 2.9", (),).is_empty());
    assert!(query_ids(
        &db,
        "SELECT * FROM pk_float_rows WHERE id = 9223372036854775808.0",
        (),
    )
    .is_empty());

    // The first execution compiles the PK shortcut. Every later parameter must
    // still pass exact admission instead of being truncated by the cached plan.
    let select = db
        .prepare("SELECT * FROM pk_float_rows WHERE id = $1")
        .unwrap();
    assert_eq!(select.query_opt::<i64, _>((3.0,)).unwrap(), Some(3));
    assert_eq!(select.query_opt::<i64, _>((3.9,)).unwrap(), None);
    assert_eq!(
        select.query_opt::<i64, _>((i64::MAX as f64,)).unwrap(),
        None
    );
    assert_eq!(select.query_opt::<i64, _>((4.0,)).unwrap(), Some(4));

    let named_sql = "SELECT * FROM pk_float_rows WHERE id = :target";
    let named_valid: Vec<i64> = db
        .query_named(named_sql, named_params! { target: 5.0 })
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect();
    assert_eq!(named_valid, vec![5]);
    assert_eq!(
        db.query_named(named_sql, named_params! { target: 5.5 })
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn update_and_delete_pk_fast_paths_never_truncate_float_parameters() {
    let db = setup("pk_float_dml");

    let update = db
        .prepare("UPDATE pk_float_rows SET payload = 'updated' WHERE id = $1")
        .unwrap();
    assert_eq!(update.execute((10.0,)).unwrap(), 1);
    assert_eq!(update.execute((10.9,)).unwrap(), 0);
    let payload: String = db
        .query_one("SELECT payload FROM pk_float_rows WHERE id = 10", ())
        .unwrap();
    assert_eq!(payload, "updated");

    let named_update = "UPDATE pk_float_rows SET payload = 'named' WHERE id = :target";
    assert_eq!(
        db.execute_named(named_update, named_params! { target: 11.0 })
            .unwrap(),
        1
    );
    assert_eq!(
        db.execute_named(named_update, named_params! { target: 11.9 })
            .unwrap(),
        0
    );

    let delete = db
        .prepare("DELETE FROM pk_float_rows WHERE id = $1")
        .unwrap();
    assert_eq!(delete.execute((12.0,)).unwrap(), 1);
    assert_eq!(delete.execute((13.9,)).unwrap(), 0);
    assert_eq!(delete.execute((i64::MAX as f64,)).unwrap(), 0);
    assert_eq!(delete.execute((13.0,)).unwrap(), 1);

    let remaining: i64 = db
        .query_one("SELECT COUNT(*) FROM pk_float_rows WHERE id = 13", ())
        .unwrap();
    assert_eq!(remaining, 0);
    let max_remaining: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM pk_float_rows WHERE id = 9223372036854775807",
            (),
        )
        .unwrap();
    assert_eq!(max_remaining, 1);
}

#[test]
fn keyset_shortcuts_preserve_original_float_semantics() {
    let db = setup("pk_float_keyset");

    let keyset = db
        .prepare("SELECT id FROM pk_float_rows WHERE id >= $1 ORDER BY id LIMIT 3")
        .unwrap();
    assert_eq!(
        keyset
            .query((1.0,))
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        keyset
            .query((1.9,))
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert!(keyset.query((i64::MAX as f64,)).unwrap().next().is_none());

    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM pk_float_rows WHERE id >= 1.9 ORDER BY id LIMIT 3",
            (),
        ),
        vec![2, 3, 4]
    );
    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM pk_float_rows WHERE 1.9 <= id ORDER BY id LIMIT 3",
            (),
        ),
        vec![2, 3, 4]
    );
    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM pk_float_rows WHERE id >= ($1 + 0.9) ORDER BY id LIMIT 3",
            (1.0,),
        ),
        vec![2, 3, 4]
    );

    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM pk_float_rows WHERE id > 1 AND id > 5 ORDER BY id LIMIT 3",
            (),
        ),
        vec![6, 7, 8]
    );
    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM pk_float_rows WHERE id > 5 AND id > 1 ORDER BY id LIMIT 3",
            (),
        ),
        vec![6, 7, 8]
    );

    let conjunctive_keyset = db
        .prepare("SELECT id FROM pk_float_rows WHERE id > $1 AND id >= $2 ORDER BY id LIMIT 3")
        .unwrap();
    for (params, expected) in [((1_i64, 6_i64), vec![6, 7, 8]), ((5, 1), vec![6, 7, 8])] {
        assert_eq!(
            conjunctive_keyset
                .query(params)
                .unwrap()
                .map(|row| row.unwrap().get::<i64>(0).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
}
