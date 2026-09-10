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

//! JR-13 adversarial matrix for logical boundaries that optimized equality
//! JOINs must not blur.

use radixdb::{Database, Error, Result, Value};

fn database(name: &str) -> Database {
    Database::open(&format!("memory://jr13-{name}")).expect("open JR-13 database")
}

fn rows(database: &Database, sql: &str) -> Result<Vec<Vec<Value>>> {
    database
        .query(sql, ())?
        .map(|row| row.map(|row| row.into_inner().as_slice().to_vec()))
        .collect()
}

fn explain(database: &Database, sql: &str) -> Result<String> {
    Ok(database
        .query(&format!("EXPLAIN ANALYZE {sql}"), ())?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?
        .join("\n"))
}

#[test]
fn left_residual_match_state_handles_zero_one_many_duplicates_and_null_keys() -> Result<()> {
    let db = database("outer-residual-matrix");
    db.execute(
        "CREATE TABLE l (id INTEGER PRIMARY KEY, k INTEGER, threshold INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE r (id INTEGER PRIMARY KEY, k INTEGER, score INTEGER)",
        (),
    )?;
    db.execute(
        "INSERT INTO l VALUES (1,1,5),(2,2,5),(3,3,5),(4,4,5),(5,NULL,5)",
        (),
    )?;
    db.execute(
        "INSERT INTO r VALUES
         (10,1,4),(11,1,6),
         (20,2,4),(21,2,3),
         (40,4,6),(41,4,7),
         (50,NULL,9)",
        (),
    )?;

    let sql = "SELECT l.id, r.id FROM l LEFT JOIN r \
               ON l.k = r.k AND l.threshold < r.score \
               ORDER BY l.id, r.id NULLS LAST";
    assert_eq!(
        rows(&db, sql)?,
        vec![
            vec![Value::Integer(1), Value::Integer(11)],
            vec![Value::Integer(2), Value::null(radixdb::DataType::Integer)],
            vec![Value::Integer(3), Value::null(radixdb::DataType::Integer)],
            vec![Value::Integer(4), Value::Integer(40)],
            vec![Value::Integer(4), Value::Integer(41)],
            vec![Value::Integer(5), Value::null(radixdb::DataType::Integer)],
        ]
    );
    let plan = explain(&db, sql)?;
    assert!(plan.contains("hash_streaming=1"), "{plan}");
    assert!(plan.contains("nested_loop=0"), "{plan}");

    assert_eq!(
        rows(
            &db,
            "SELECT l.id, r.id FROM l INNER JOIN r \
             ON l.k = r.k AND l.threshold < r.score ORDER BY l.id, r.id",
        )?,
        vec![
            vec![Value::Integer(1), Value::Integer(11)],
            vec![Value::Integer(4), Value::Integer(40)],
            vec![Value::Integer(4), Value::Integer(41)],
        ],
        "INNER must retain duplicate non-unique matches and reject NULL/missing/residual failures"
    );
    db.close()?;
    Ok(())
}

#[test]
fn on_predicates_where_predicates_or_and_is_null_keep_distinct_semantics() -> Result<()> {
    let db = database("on-versus-where");
    db.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, k INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, k INTEGER, enabled INTEGER, note TEXT)",
        (),
    )?;
    db.execute(
        "INSERT INTO parent VALUES (1,10),(2,20),(3,30),(4,NULL)",
        (),
    )?;
    db.execute(
        "INSERT INTO child VALUES
         (100,10,1,'enabled'),(101,10,0,'disabled'),
         (200,20,0,'disabled'),(400,NULL,1,'null-key')",
        (),
    )?;

    assert_eq!(
        rows(
            &db,
            "SELECT p.id, c.id FROM parent p LEFT JOIN child c \
             ON p.k=c.k AND c.enabled=1 ORDER BY p.id, c.id NULLS LAST",
        )?,
        vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::null(radixdb::DataType::Integer)],
            vec![Value::Integer(3), Value::null(radixdb::DataType::Integer)],
            vec![Value::Integer(4), Value::null(radixdb::DataType::Integer)],
        ],
        "ON residual must preserve the left row"
    );
    assert_eq!(
        rows(
            &db,
            "SELECT p.id, c.id FROM parent p LEFT JOIN child c ON p.k=c.k \
             WHERE c.enabled=1 ORDER BY p.id, c.id",
        )?,
        vec![vec![Value::Integer(1), Value::Integer(100)]],
        "nullable-side WHERE predicate must filter NULL-extended rows"
    );
    assert_eq!(
        rows(
            &db,
            "SELECT p.id FROM parent p LEFT JOIN child c \
             ON p.k=c.k AND (c.enabled=1 OR c.note='never') \
             WHERE c.id IS NULL ORDER BY p.id",
        )?,
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
        ]
    );
    db.close()?;
    Ok(())
}

#[test]
fn schema_binding_uses_proven_owner_and_rejects_ambiguous_self_join_columns() -> Result<()> {
    let db = database("binding");
    db.execute(
        "CREATE TABLE roots (id INTEGER PRIMARY KEY, only_root TEXT, next_id INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE targets (id INTEGER PRIMARY KEY, only_target TEXT)",
        (),
    )?;
    db.execute("INSERT INTO roots VALUES (1,'keep',2),(2,'drop',NULL)", ())?;
    db.execute("INSERT INTO targets VALUES (2,'target')", ())?;

    assert_eq!(
        rows(
            &db,
            "SELECT r.id, t.only_target FROM roots r JOIN targets t ON t.id=r.next_id \
             WHERE only_root='keep'",
        )?,
        vec![vec![Value::Integer(1), Value::text("target")]],
        "unqualified unique column must bind to its schema-proven owner"
    );
    let ambiguous = db.query(
        "SELECT r.id FROM roots r JOIN targets t ON t.id=r.next_id WHERE id=1",
        (),
    );
    assert!(matches!(ambiguous, Err(Error::AmbiguousColumn(column)) if column == "id"));

    let ambiguous_projection = db.query(
        "SELECT id, only_target FROM roots r JOIN targets t ON t.id=r.next_id",
        (),
    );
    assert!(matches!(
        ambiguous_projection,
        Err(Error::AmbiguousColumn(column)) if column == "id"
    ));

    let ambiguous_empty_projection = db.query(
        "SELECT id FROM roots r JOIN targets t ON t.id=r.next_id WHERE 1=0",
        (),
    );
    assert!(matches!(
        ambiguous_empty_projection,
        Err(Error::AmbiguousColumn(column)) if column == "id"
    ));

    let ambiguous_order = db.query(
        "SELECT r.id AS root_id, t.only_target
         FROM roots r JOIN targets t ON t.id=r.next_id ORDER BY id",
        (),
    );
    assert!(matches!(
        ambiguous_order,
        Err(Error::AmbiguousColumn(column)) if column == "id"
    ));

    assert_eq!(
        rows(
            &db,
            "SELECT r.id, t.only_target FROM roots r JOIN targets t ON t.id=r.next_id",
        )?,
        vec![vec![Value::Integer(1), Value::text("target")]],
        "a qualified retry must remain usable after the rejected statement"
    );
    assert_eq!(
        rows(
            &db,
            "SELECT r.id AS id, t.only_target
             FROM roots r JOIN targets t ON t.id=r.next_id ORDER BY id",
        )?,
        vec![vec![Value::Integer(1), Value::text("target")]],
        "a unique SELECT output label must retain ORDER BY precedence"
    );
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM roots r JOIN targets t USING(id) WHERE id=2 ORDER BY id",
        )?,
        vec![vec![Value::Integer(2)]],
        "USING must publish its join key as one unambiguous column"
    );
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM roots r NATURAL JOIN targets t WHERE id=2 ORDER BY id",
        )?,
        vec![vec![Value::Integer(2)]],
        "NATURAL JOIN must publish each common key once"
    );

    assert_eq!(
        rows(
            &db,
            "SELECT child.id, parent.id FROM roots child \
             LEFT JOIN roots parent ON parent.id=child.next_id ORDER BY child.id",
        )?,
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::null(radixdb::DataType::Integer)],
        ]
    );
    db.close()?;
    Ok(())
}

#[test]
fn right_full_cross_view_cte_subquery_and_correlated_boundaries_execute_exactly() -> Result<()> {
    let db = database("boundaries");
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute("INSERT INTO a VALUES (1,10),(2,20)", ())?;
    db.execute("INSERT INTO b VALUES (10,10),(30,30)", ())?;

    assert_eq!(
        rows(
            &db,
            "SELECT a.id,b.id FROM a RIGHT JOIN b ON a.k=b.k ORDER BY b.id",
        )?,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::null(radixdb::DataType::Integer), Value::Integer(30)],
        ]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT a.id,b.id FROM a FULL JOIN b ON a.k=b.k \
             ORDER BY a.id NULLS LAST,b.id NULLS LAST",
        )?,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::null(radixdb::DataType::Integer)],
            vec![Value::null(radixdb::DataType::Integer), Value::Integer(30)],
        ]
    );
    let cross: i64 = db.query_one("SELECT COUNT(*) FROM a CROSS JOIN b", ())?;
    assert_eq!(cross, 4);

    db.execute(
        "CREATE VIEW matched_v AS SELECT a.id AS aid,b.id AS bid FROM a JOIN b ON a.k=b.k",
        (),
    )?;
    for sql in [
        "SELECT aid,bid FROM matched_v",
        "WITH matched AS (SELECT a.id AS aid,b.id AS bid FROM a JOIN b ON a.k=b.k) SELECT aid,bid FROM matched",
        "SELECT q.aid,q.bid FROM (SELECT a.id AS aid,b.id AS bid FROM a JOIN b ON a.k=b.k) q",
        "SELECT a.id, (SELECT b.id FROM b WHERE b.k=a.k) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
    ] {
        assert_eq!(
            rows(&db, sql)?,
            vec![vec![Value::Integer(1), Value::Integer(10)]],
            "boundary query changed semantics: {sql}"
        );
    }
    db.close()?;
    Ok(())
}

#[test]
fn index_ddl_generation_transaction_rollback_and_schema_change_keep_join_correct() -> Result<()> {
    let db = database("ddl-generation");
    db.execute(
        "CREATE TABLE outer_rows (id INTEGER PRIMARY KEY, lookup_key INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE target_rows (id INTEGER PRIMARY KEY, lookup_key INTEGER, payload TEXT)",
        (),
    )?;
    db.execute(
        "CREATE UNIQUE INDEX target_lookup_uq ON target_rows(lookup_key)",
        (),
    )?;
    db.execute("INSERT INTO outer_rows VALUES (1,10)", ())?;
    db.execute("INSERT INTO target_rows VALUES (100,10,'committed')", ())?;
    let sql = "SELECT o.id,t.payload FROM outer_rows o JOIN target_rows t \
               ON t.lookup_key=o.lookup_key ORDER BY o.id";
    assert_eq!(
        rows(&db, sql)?,
        vec![vec![Value::Integer(1), Value::text("committed")]]
    );

    db.execute("BEGIN", ())?;
    db.execute("INSERT INTO outer_rows VALUES (2,20)", ())?;
    db.execute("INSERT INTO target_rows VALUES (200,20,'private')", ())?;
    assert_eq!(rows(&db, sql)?.len(), 2, "explicit transaction RYW");
    db.execute("ROLLBACK", ())?;
    assert_eq!(
        rows(&db, sql)?.len(),
        1,
        "rollback must remove private edge"
    );

    db.execute(
        "ALTER INDEX target_lookup_uq RENAME TO target_lookup_renamed",
        (),
    )?;
    assert_eq!(rows(&db, sql)?.len(), 1, "renamed index generation");
    db.execute("DROP INDEX target_lookup_renamed ON target_rows", ())?;
    assert_eq!(rows(&db, sql)?.len(), 1, "scan fallback after index drop");
    assert!(
        !explain(&db, sql)?.contains("target_lookup_renamed"),
        "dropped index survived in the physical plan"
    );

    db.execute(
        "ALTER TABLE target_rows ADD COLUMN revision INTEGER DEFAULT 1",
        (),
    )?;
    db.execute(
        "UPDATE target_rows SET payload='schema-changed',revision=2 WHERE id=100",
        (),
    )?;
    assert_eq!(
        rows(&db, sql)?,
        vec![vec![Value::Integer(1), Value::text("schema-changed")]],
        "schema generation change invalidated JOIN projection"
    );
    db.close()?;
    Ok(())
}
