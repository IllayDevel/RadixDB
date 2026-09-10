//! ORM facade performance gate.
//!
//! Every server-side case compares canonical ORM IR with the equivalent raw
//! SQL against the same embedded database. Client-only cases isolate IR/JSON
//! generation so renderer overhead is not confused with execution time.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use radixdb::{Database, Rows, Value};
use radixdb_orm::{
    table, table_as, DeleteBuilder, Expr, Grouping, InsertBuilder, IrDocument, OrmBuilder,
    QueryBuilder, UpdateBuilder,
};

fn consume(rows: Rows) -> usize {
    rows.fold(0, |count, row| {
        row.expect("benchmark row");
        count + 1
    })
}

fn fixture() -> Database {
    let db = Database::open_in_memory().expect("open benchmark database");
    db.execute(
        "CREATE TABLE fio (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\
         CREATE TABLE people (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             active BOOLEAN NOT NULL,\
             score INTEGER NOT NULL,\
             fio_id INTEGER REFERENCES fio(id)\
         )",
        (),
    )
    .expect("create benchmark schema");
    db.execute("BEGIN", ()).unwrap();
    for id in 0_i64..100 {
        db.execute(
            "INSERT INTO fio VALUES (?, ?)",
            (id, format!("dictionary-{id}")),
        )
        .unwrap();
    }
    for id in 0_i64..10_000 {
        db.execute(
            "INSERT INTO people VALUES (?, ?, ?, ?, ?)",
            (
                id,
                format!("person-{id}"),
                id % 3 != 0,
                id % 1_000,
                id % 100,
            ),
        )
        .unwrap();
    }
    db.execute("COMMIT", ()).unwrap();
    db
}

fn get_document(id: i64) -> IrDocument {
    QueryBuilder::from_relation(table("people"))
        .select([Expr::column("id"), Expr::column("name")])
        .filter(Expr::column("id").eq(id))
        .document()
        .unwrap()
}

fn complex_document(value: i64) -> IrDocument {
    QueryBuilder::from_relation(table_as("people", "p"))
        .select([
            Expr::qualified("p", "id"),
            Expr::qualified("p", "name"),
            Expr::navigation("p", ["fio_id", "name"]),
        ])
        .filter(
            Expr::qualified("p", "active")
                .eq(true)
                .and(Expr::qualified("p", "score").between(value, value + 50))
                .and(Expr::qualified("p", "name").like("person-%")),
        )
        .order_by([Expr::qualified("p", "score").desc().nulls_last()])
        .limit(100)
        .document()
        .unwrap()
}

fn batch_insert_document() -> IrDocument {
    InsertBuilder::new("people")
        .rows(
            vec![
                "id".into(),
                "name".into(),
                "active".into(),
                "score".into(),
                "fio_id".into(),
            ],
            (20_000_i64..20_100)
                .map(|id| {
                    vec![
                        Expr::value(id),
                        Expr::value(format!("batch-{id}")),
                        Expr::value(true),
                        Expr::value(id),
                        Expr::value(id % 100),
                    ]
                })
                .collect(),
        )
        .document()
        .unwrap()
}

fn benchmark_client(c: &mut Criterion) {
    let mut group = c.benchmark_group("orm.client");
    group.bench_function("get_ir_and_sql", |b| {
        b.iter(|| black_box(get_document(black_box(5_000))).to_sql().unwrap())
    });
    group.bench_function("complex_ir_and_sql", |b| {
        b.iter(|| {
            black_box(complex_document(black_box(250)))
                .to_sql()
                .unwrap()
        })
    });
    let document = complex_document(250);
    group.bench_function("json_round_trip", |b| {
        b.iter(|| {
            let json = black_box(&document).to_json().unwrap();
            black_box(IrDocument::from_json(&json).unwrap())
        })
    });
    group.bench_function("batch_insert_100_ir_and_sql", |b| {
        b.iter(|| black_box(batch_insert_document().to_sql().unwrap()))
    });
    group.finish();
}

fn benchmark_queries(c: &mut Criterion) {
    let db = fixture();
    let get = get_document(5_000);
    let classic = QueryBuilder::from_relation(table_as("people", "p"))
        .left_join(
            table_as("fio", "f"),
            Expr::qualified("p", "fio_id").eq(Expr::qualified("f", "id")),
        )
        .select([Expr::qualified("p", "id"), Expr::qualified("f", "name")])
        .filter(Expr::qualified("p", "id").between(4_900_i64, 5_100_i64))
        .document()
        .unwrap();
    let navigation = QueryBuilder::from_relation(table_as("people", "p"))
        .select([
            Expr::qualified("p", "id"),
            Expr::navigation("p", ["fio_id", "name"]),
        ])
        .filter(Expr::qualified("p", "id").between(4_900_i64, 5_100_i64))
        .document()
        .unwrap();
    let aggregate = QueryBuilder::from_relation(table("people"))
        .select([
            Expr::column("active"),
            Expr::aggregate("COUNT", [Expr::star()], false, None, vec![]),
        ])
        .group_by(Grouping::Cube {
            expressions: vec![Expr::column("active").0],
        })
        .document()
        .unwrap();
    let window = QueryBuilder::from_relation(table("people"))
        .select([
            Expr::column("id"),
            Expr::function("ROW_NUMBER", []).window(radixdb_orm::WindowSpecification {
                name: None,
                partition_by: vec![Expr::column("active").0],
                order_by: vec![Expr::column("score").desc().0],
                frame: None,
            }),
        ])
        .limit(1_000)
        .document()
        .unwrap();

    let mut group = c.benchmark_group("orm.execution");
    group.bench_function("get.raw", |b| {
        b.iter(|| {
            black_box(consume(
                db.query("SELECT id, name FROM people WHERE id = ?", (5_000_i64,))
                    .unwrap(),
            ))
        })
    });
    group.bench_function("get.orm", |b| {
        b.iter(|| black_box(consume(db.query_orm(black_box(&get)).unwrap())))
    });
    group.bench_function("classic_join.raw", |b| {
        b.iter(|| {
            black_box(consume(
                db.query(
                    "SELECT p.id, f.name FROM people p LEFT JOIN fio f ON p.fio_id = f.id WHERE p.id BETWEEN ? AND ?",
                    (4_900_i64, 5_100_i64),
                )
                .unwrap(),
            ))
        })
    });
    group.bench_function("classic_join.orm", |b| {
        b.iter(|| black_box(consume(db.query_orm(black_box(&classic)).unwrap())))
    });
    group.bench_function("navigation.raw", |b| {
        b.iter(|| {
            black_box(consume(
                db.query(
                    "SELECT p.id, p.fio_id.name FROM people p WHERE p.id BETWEEN ? AND ?",
                    (4_900_i64, 5_100_i64),
                )
                .unwrap(),
            ))
        })
    });
    group.bench_function("navigation.orm", |b| {
        b.iter(|| black_box(consume(db.query_orm(black_box(&navigation)).unwrap())))
    });
    group.bench_function("cube.raw", |b| {
        b.iter(|| {
            black_box(consume(
                db.query(
                    "SELECT active, COUNT(*) FROM people GROUP BY CUBE(active)",
                    (),
                )
                .unwrap(),
            ))
        })
    });
    group.bench_function("cube.orm", |b| {
        b.iter(|| black_box(consume(db.query_orm(black_box(&aggregate)).unwrap())))
    });
    group.bench_function("window.raw", |b| {
        b.iter(|| {
            black_box(consume(
                db.query(
                    "SELECT id, ROW_NUMBER() OVER (PARTITION BY active ORDER BY score DESC) FROM people LIMIT 1000",
                    (),
                )
                .unwrap(),
            ))
        })
    });
    group.bench_function("window.orm", |b| {
        b.iter(|| black_box(consume(db.query_orm(black_box(&window)).unwrap())))
    });
    group.finish();
}

fn benchmark_mutations(c: &mut Criterion) {
    let db = fixture();
    let insert = InsertBuilder::new("people")
        .value("id", 20_000_i64)
        .value("name", "inserted")
        .value("active", true)
        .value("score", 1_i64)
        .value("fio_id", 1_i64)
        .document()
        .unwrap();
    let update = UpdateBuilder::new("people")
        .set("score", 777_i64)
        .filter(Expr::column("id").eq(5_000_i64))
        .document()
        .unwrap();
    let save = InsertBuilder::new("people")
        .value("id", 5_000_i64)
        .value("name", "saved")
        .value("active", true)
        .value("score", 888_i64)
        .value("fio_id", 1_i64)
        .on_conflict(["id"])
        .do_update("name", Expr::qualified("excluded", "name"))
        .document()
        .unwrap();
    let delete = DeleteBuilder::new("people")
        .filter(Expr::column("id").eq(5_000_i64))
        .document()
        .unwrap();
    let batch = batch_insert_document();
    let batch_sql = format!(
        "INSERT INTO people (id, name, active, score, fio_id) VALUES {}",
        std::iter::repeat_n("(?, ?, ?, ?, ?)", 100)
            .collect::<Vec<_>>()
            .join(", ")
    );
    let batch_parameters = (20_000_i64..20_100)
        .flat_map(|id| {
            [
                Value::Integer(id),
                Value::Text(format!("batch-{id}").into()),
                Value::Boolean(true),
                Value::Integer(id),
                Value::Integer(id % 100),
            ]
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("orm.mutation");
    for (name, sql, document) in [
        (
            "insert",
            "INSERT INTO people VALUES (20000, 'inserted', TRUE, 1, 1)",
            insert,
        ),
        (
            "update",
            "UPDATE people SET score = 777 WHERE id = 5000",
            update,
        ),
        (
            "save",
            "INSERT INTO people VALUES (5000, 'saved', TRUE, 888, 1) ON CONFLICT (id) DO UPDATE SET name = excluded.name",
            save,
        ),
        (
            "delete",
            "DELETE FROM people WHERE id = 5000",
            delete,
        ),
    ] {
        group.bench_function(format!("{name}.raw"), |b| {
            b.iter(|| {
                db.execute("BEGIN", ()).unwrap();
                black_box(db.execute(sql, ()).unwrap());
                db.execute("ROLLBACK", ()).unwrap();
            })
        });
        group.bench_function(format!("{name}.orm"), |b| {
            b.iter(|| {
                db.execute("BEGIN", ()).unwrap();
                black_box(db.execute_orm(&document).unwrap());
                db.execute("ROLLBACK", ()).unwrap();
            })
        });
    }
    group.bench_function("batch_insert_100.raw", |b| {
        b.iter(|| {
            db.execute("BEGIN", ()).unwrap();
            black_box(
                db.execute(batch_sql.as_str(), batch_parameters.clone())
                    .unwrap(),
            );
            db.execute("ROLLBACK", ()).unwrap();
        })
    });
    group.bench_function("batch_insert_100.orm", |b| {
        b.iter(|| {
            db.execute("BEGIN", ()).unwrap();
            black_box(db.execute_orm(&batch).unwrap());
            db.execute("ROLLBACK", ()).unwrap();
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_client,
    benchmark_queries,
    benchmark_mutations
);
criterion_main!(benches);
