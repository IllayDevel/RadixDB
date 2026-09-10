use std::collections::BTreeMap;

use radixdb::parser::parse_sql;
use radixdb::{Database, Result};
use radixdb_orm::*;

fn assert_document_round_trip_and_parse(document: IrDocument) {
    let json = document.to_json().expect("IR JSON");
    assert_eq!(
        IrDocument::from_json(&json).expect("IR round-trip"),
        document
    );
    let compiled = document.to_sql().expect("SQL render");
    let statements = parse_sql(&compiled.sql)
        .unwrap_or_else(|error| panic!("generated SQL did not parse: {}\n{error}", compiled.sql));
    assert_eq!(statements.len(), 1, "{}", compiled.sql);
    assert!(
        !compiled.sql.contains("<bound:"),
        "runtime-only AST escaped into SQL"
    );
}

fn relation(name: &str, alias: &str) -> Relation {
    table_as(name, alias)
}

#[test]
fn orm_ir_json_and_sql_cover_the_complete_public_surface() {
    let create = DdlBuilder::create_table("all_types")
        .column(Column::integer("id").primary_key(true))
        .column(Column::float("float_value"))
        .column(Column::decimal("decimal_value", 38, 10))
        .column(Column::text("text_value").default("server"))
        .column(Column::boolean("boolean_value"))
        .column(Column::timestamp("timestamp_value"))
        .column(Column::date("date_value"))
        .column(Column::json("json_value"))
        .column(Column::uuid("uuid_value"))
        .column(Column::bytes("bytes_value"))
        .column(Column::vector("embedding", 3))
        .constraint(unique(["text_value", "uuid_value"]))
        .constraint(check(Expr::column("id").gte(0_i64)));
    assert_document_round_trip_and_parse(create.document().unwrap());

    assert_document_round_trip_and_parse(
        DdlBuilder::create_index(IndexDefinition {
            name: "idx_all_types_embedding".to_string(),
            table: "all_types".to_string(),
            columns: vec!["embedding".to_string()],
            unique: false,
            if_not_exists: false,
            method: Some("HNSW".to_string()),
            predicate: None,
            options: BTreeMap::from([
                ("m".to_string(), TypedValue::Integer(8)),
                ("metric".to_string(), TypedValue::Text("cosine".to_string())),
            ]),
        })
        .document()
        .unwrap(),
    );

    let payroll = QueryBuilder::from_relation(relation("payroll_documents", "d"))
        .select([Expr::qualified("d", "employee_id")])
        .filter(
            Expr::qualified("d", "status")
                .in_list(["posted", "paid"])
                .and(Expr::qualified("d", "amount").between(
                    TypedValue::Decimal("0.00".to_string()),
                    TypedValue::Decimal("999999.99".to_string()),
                )),
        );
    let cte = CommonTableExpression {
        name: "paid".to_string(),
        columns: vec!["employee_id".to_string()],
        query: Box::new(payroll.into_select()),
    };
    let aggregate = Expr::aggregate(
        "STRING_AGG",
        [Expr::navigation("p", ["fio", "name"])],
        true,
        Some(Expr::qualified("p", "active").eq(true)),
        vec![Expr::navigation("p", ["fio", "name"]).asc().nulls_last()],
    );
    let ranked = Expr::function("ROW_NUMBER", [])
        .window(WindowSpecification {
            name: None,
            partition_by: vec![Expr::navigation("p", ["position_id", "department_id", "name"]).0],
            order_by: vec![Expr::qualified("p", "hired_at").desc().nulls_last().0],
            frame: Some(WindowFrame {
                unit: WindowFrameUnit::Rows,
                start: WindowFrameBound::UnboundedPreceding,
                end: Some(WindowFrameBound::CurrentRow),
            }),
        })
        .alias("rn");
    let exists = Expr::exists(
        QueryBuilder::from_relation(Relation::Cte {
            name: "paid".to_string(),
            alias: Some("x".to_string()),
        })
        .select([Expr::value(1_i64)])
        .filter(Expr::qualified("x", "employee_id").eq(Expr::qualified("p", "id"))),
    );
    let select = QueryBuilder::from_relation(relation("people", "p"))
        .with_cte(cte)
        .select_projections(vec![
            Expr::qualified("p", "id").projection(),
            Expr::navigation("p", ["fio", "name"]).alias("fio_name"),
            aggregate.alias("names"),
            ranked,
            Expr::grouping([Expr::navigation(
                "p",
                ["position_id", "department_id", "region"],
            )])
            .alias("grouping_mask"),
        ])
        .filter(exists)
        .group_by(Grouping::Cube {
            expressions: vec![
                Expr::qualified("p", "id").0,
                Expr::navigation("p", ["fio", "name"]).0,
                Expr::navigation("p", ["position_id", "department_id", "region"]).0,
                Expr::qualified("p", "hired_at").0,
            ],
        })
        .having(Expr::aggregate("COUNT", [Expr::star()], false, None, vec![]).gt(0_i64))
        .order_by([Expr::qualified("p", "id").asc().nulls_last()])
        .limit(50)
        .offset(2);
    assert_document_round_trip_and_parse(select.document().unwrap());

    let first = QueryBuilder::from_relation(table("people"))
        .select([Expr::column("id")])
        .filter(Expr::column("active").eq(true));
    let second = QueryBuilder::from_relation(table("archived_people")).select([Expr::column("id")]);
    let set = first
        .set_operation(SetOperator::UnionAll, second.clone())
        .set_operation(SetOperator::Intersect, second.clone())
        .set_operation(SetOperator::Except, second);
    assert_document_round_trip_and_parse(set.document().unwrap());
    assert_document_round_trip_and_parse(set.explain(true).unwrap());

    let insert = InsertBuilder::new("people")
        .value(
            "id",
            TypedValue::Uuid("018c0e27-aa31-7000-8000-112233445566".into()),
        )
        .value("email", "user@example.test")
        .returning_all();
    assert_document_round_trip_and_parse(insert.document().unwrap());
    assert_document_round_trip_and_parse(
        insert
            .clone()
            .on_conflict(["id"])
            .do_update("email", Expr::qualified("excluded", "email"))
            .document()
            .unwrap(),
    );
    assert_document_round_trip_and_parse(
        UpdateBuilder::new("people")
            .set("active", false)
            .filter(Expr::column("id").eq(1_i64))
            .returning_all()
            .document()
            .unwrap(),
    );
    assert_document_round_trip_and_parse(
        DeleteBuilder::new("people")
            .alias("p")
            .filter(Expr::qualified("p", "id").eq(1_i64))
            .returning_all()
            .document()
            .unwrap(),
    );
    assert_document_round_trip_and_parse(IrDocument::new(Operation::Transaction {
        statement: TransactionOperation::RollbackToSavepoint {
            name: "orm_savepoint".to_string(),
        },
    }));
}

fn collect_people(rows: radixdb::Rows) -> Result<Vec<(i64, Option<String>)>> {
    rows.map(|row| {
        let row = row?;
        Ok((row.get(0)?, row.get(1)?))
    })
    .collect()
}

#[test]
fn orm_generated_navigation_and_classic_join_match_manual_sql() -> Result<()> {
    let db = Database::open_in_memory()?;
    db.execute(
        "CREATE TABLE fio (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\
         CREATE TABLE people (id INTEGER PRIMARY KEY, fio INTEGER REFERENCES fio(id), active BOOLEAN NOT NULL DEFAULT TRUE)",
        (),
    )?;
    db.execute("INSERT INTO fio VALUES (10, 'Ivan'), (20, 'Petr')", ())?;
    db.execute(
        "INSERT INTO people(id, fio) VALUES (1, 10), (2, 20), (3, NULL)",
        (),
    )?;

    let manual = collect_people(db.query(
        "SELECT p.id, f.name FROM people p LEFT JOIN fio f ON p.fio = f.id ORDER BY p.id",
        (),
    )?)?;
    let navigation = QueryBuilder::from_relation(relation("people", "p"))
        .select([
            Expr::qualified("p", "id"),
            Expr::navigation("p", ["fio", "name"]),
        ])
        .order_by([Expr::qualified("p", "id").asc()]);
    let navigation_rows = collect_people(db.query_orm(&navigation.document().unwrap()).unwrap())?;
    assert_eq!(navigation_rows, manual);

    let classic = QueryBuilder::from_relation(relation("people", "p"))
        .left_join(
            relation("fio", "f"),
            Expr::qualified("p", "fio").eq(Expr::qualified("f", "id")),
        )
        .select([Expr::qualified("p", "id"), Expr::qualified("f", "name")])
        .order_by([Expr::qualified("p", "id").asc()]);
    let classic_rows = collect_people(db.query_orm(&classic.document().unwrap()).unwrap())?;
    assert_eq!(classic_rows, manual);
    Ok(())
}

#[test]
fn orm_ir_property_matrix_round_trips_without_value_interpolation() {
    let mut shape = None;
    for seed in 0_i64..128 {
        let secret = format!("value-'-$1-{seed}");
        let query = QueryBuilder::from_relation(relation("people", "p"))
            .select([
                Expr::qualified("p", "id"),
                Expr::case(
                    None,
                    [(
                        Expr::qualified("p", "score").gte(seed),
                        Expr::value(secret.clone()),
                    )],
                    Some(Expr::value("fallback")),
                ),
            ])
            .filter(
                Expr::qualified("p", "id")
                    .between(seed - 1, seed + 1)
                    .and(Expr::qualified("p", "name").ne(secret.clone())),
            )
            .order_by([Expr::qualified("p", "id").desc().nulls_last()])
            .limit(3);
        let document = query.document().unwrap();
        let json = document.to_json().unwrap();
        assert_eq!(IrDocument::from_json(&json).unwrap(), document);
        let redacted = document.to_redacted_json().unwrap();
        assert!(!redacted.contains(&secret));
        let compiled = document.to_sql().unwrap();
        assert!(!compiled.sql.contains(&secret));
        assert_eq!(parse_sql(&compiled.sql).unwrap().len(), 1);
        assert_eq!(compiled.parameters.len(), 6);
        match &shape {
            Some(expected) => assert_eq!(&compiled.shape_fingerprint, expected),
            None => shape = Some(compiled.shape_fingerprint),
        }
    }

    assert!(UpdateBuilder::new("people")
        .alias("p")
        .set("name", "x")
        .filter(Expr::qualified("p", "id").eq(1_i64))
        .to_sql()
        .is_err());
    assert!(DeleteBuilder::new("people")
        .using(table("other"))
        .filter(Expr::column("id").eq(1_i64))
        .to_sql()
        .is_err());
}

#[cfg(feature = "sqlite")]
#[test]
fn orm_generated_common_subset_matches_sqlite() -> Result<()> {
    let db = Database::open_in_memory()?;
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    let schema = "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)";
    db.execute(schema, ())?;
    sqlite.execute_batch(schema).unwrap();
    for row in [
        "INSERT INTO people VALUES (1, 'Alice', 10)",
        "INSERT INTO people VALUES (2, 'Bob', 20)",
        "INSERT INTO people VALUES (3, 'Alina', 30)",
        "INSERT INTO people VALUES (4, NULL, 40)",
    ] {
        db.execute(row, ())?;
        sqlite.execute_batch(row).unwrap();
    }

    let query = QueryBuilder::from_relation(table("people"))
        .select([Expr::column("id"), Expr::column("name")])
        .filter(
            Expr::column("score")
                .gte(15_i64)
                .and(Expr::column("name").like("Ali%")),
        )
        .order_by([Expr::column("id").asc()]);
    let document = query.document().unwrap();
    let radix_rows: Vec<(i64, Option<String>)> = db
        .query_orm(&document)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();

    let compiled = document.to_sql().unwrap();
    let mut statement = sqlite.prepare(&compiled.sql).unwrap();
    let sqlite_rows: Vec<(i64, Option<String>)> = statement
        .query_map(rusqlite::params![15_i64, "Ali%"], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(radix_rows, sqlite_rows);
    Ok(())
}

#[test]
fn orm_hot_cold_reopen_wal_tail_and_constraints_preserve_one_contract() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}", temp.path().join("orm-storage").display());
    let descriptor_before = {
        let db = Database::open(&dsn)?;
        db.schema()
            .create_table("dictionary")
            .column(Column::integer("id").primary_key(true))
            .column(Column::text("name").not_null(true).unique(true))
            .execute()
            .unwrap();
        db.schema()
            .create_table("documents")
            .column(Column::integer("id").primary_key(true))
            .column(Column::integer("dictionary_id").reference(reference("dictionary", "id")))
            .column(Column::text("title").default("server-default"))
            .execute()
            .unwrap();
        db.execute("INSERT INTO dictionary VALUES (10, 'A'), (20, 'B')", ())?;

        let descriptor = db.schema().table("documents").describe().fetch().unwrap();
        let mut first = DynamicRecord::new(descriptor.clone());
        first.set("id", TypedValue::Integer(1)).unwrap();
        first.set("dictionary_id", TypedValue::Integer(10)).unwrap();
        first.insert(&db).unwrap();
        assert!(!first.is_dirty());

        let mut rejected = DynamicRecord::new(descriptor.clone());
        rejected.set("id", TypedValue::Integer(99)).unwrap();
        rejected
            .set("dictionary_id", TypedValue::Integer(999))
            .unwrap();
        assert!(rejected.insert(&db).is_err());
        assert!(rejected.is_dirty());

        db.execute("PRAGMA CHECKPOINT", ())?;
        let mut wal_tail = DynamicRecord::new(descriptor.clone());
        wal_tail.set("id", TypedValue::Integer(2)).unwrap();
        wal_tail
            .set("dictionary_id", TypedValue::Integer(20))
            .unwrap();
        wal_tail
            .set("title", TypedValue::Text("hot-tail".to_string()))
            .unwrap();
        wal_tail.insert(&db).unwrap();
        db.close()?;
        descriptor
    };

    let db = Database::open(&dsn)?;
    let descriptor_after = db.schema().table("documents").describe().fetch().unwrap();
    assert_eq!(descriptor_after, descriptor_before);
    let query = QueryBuilder::from_relation(relation("documents", "d"))
        .select([
            Expr::qualified("d", "id"),
            Expr::navigation("d", ["dictionary_id", "name"]),
            Expr::qualified("d", "title"),
        ])
        .order_by([Expr::qualified("d", "id").asc()]);
    let orm_rows: Vec<(i64, String, String)> = db
        .query_orm(&query.document().unwrap())
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            )
        })
        .collect();
    let raw_rows: Vec<(i64, String, String)> = db
        .query(
            "SELECT d.id, x.name, d.title
             FROM documents d LEFT JOIN dictionary x ON d.dictionary_id = x.id
             ORDER BY d.id",
            (),
        )?
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            )
        })
        .collect();
    assert_eq!(orm_rows, raw_rows);
    assert_eq!(orm_rows.len(), 2);

    db.schema()
        .alter_table("documents")
        .add_column(Column::text("note"))
        .execute()
        .unwrap();
    let altered = db.schema().table("documents").describe().fetch().unwrap();
    assert!(
        ensure_schema_fingerprint(&descriptor_before.fingerprint, &altered.fingerprint).is_err()
    );
    Ok(())
}
