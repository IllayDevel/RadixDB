//! Regenerate the checked-in language-neutral ORM conformance fixtures.
//!
//! The output is deterministic JSON on stdout. Repository maintenance writes
//! it to `schemas/radixdb.orm.v1.fixtures.json` and reviews the resulting diff.

use radixdb_orm::*;
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct FixtureArtifact {
    fixtures: &'static str,
    cases: Vec<FixtureCase>,
}

#[derive(Serialize)]
struct FixtureCase {
    id: &'static str,
    steps: Vec<FixtureStep>,
    result: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rejected_documents: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct FixtureStep {
    document: IrDocument,
    sql: String,
    parameters: Vec<TypedValue>,
}

impl FixtureCase {
    fn new(id: &'static str, documents: Vec<IrDocument>) -> Self {
        let steps = documents
            .into_iter()
            .map(|document| {
                let compiled = document
                    .to_sql()
                    .unwrap_or_else(|error| panic!("{id}: render failed: {error}"));
                FixtureStep {
                    document,
                    sql: compiled.sql,
                    parameters: compiled.parameters,
                }
            })
            .collect();
        Self {
            id,
            steps,
            result: json!({
                "kind": "semantic_oracle",
                "oracle": "same_rows_types_errors_as_manual_sql"
            }),
            rejected_documents: Vec::new(),
        }
    }

    fn rejected(mut self, values: Vec<serde_json::Value>) -> Self {
        self.rejected_documents = values;
        self
    }
}

fn document(builder: &impl OrmBuilder) -> IrDocument {
    builder.document().expect("fixture builder must be valid")
}

fn catalog(operation: CatalogOperation) -> IrDocument {
    IrDocument::new(Operation::Catalog { operation })
}

fn transaction(statement: TransactionOperation) -> IrDocument {
    IrDocument::new(Operation::Transaction { statement })
}

fn query(table_name: &str, projection: impl IntoIterator<Item = Expr>) -> QueryBuilder {
    QueryBuilder::from_relation(table(table_name)).select(projection)
}

fn select_star(table_name: &str) -> QueryBuilder {
    query(table_name, [Expr::star()])
}

fn insert_person(id: &str, email: &str, fio: Option<&str>) -> InsertBuilder {
    let mut insert = InsertBuilder::new("people")
        .value("id", TypedValue::Uuid(id.to_string()))
        .value("email", email);
    if let Some(fio) = fio {
        insert = insert.value("fio", TypedValue::Uuid(fio.to_string()));
    }
    insert.returning_all()
}

fn main() {
    let uuid_a = "018c0e27-aa31-7000-8000-112233445566";
    let uuid_b = "018c0e27-aa31-7000-8000-222344556677";
    let mut cases = vec![
        FixtureCase::new(
            "ORM-CONFORMANCE-001",
            vec![catalog(CatalogOperation::ListTables)],
        ),
        FixtureCase::new(
            "ORM-CONFORMANCE-002",
            vec![document(
                &InsertBuilder::new("people")
                    .value("id", 7_i64)
                    .value("name", "Alice")
                    .returning_all(),
            )],
        ),
        FixtureCase::new(
            "ORM-CONFORMANCE-003",
            vec![transaction(TransactionOperation::Savepoint {
                name: "orm_step".to_string(),
            })],
        ),
        FixtureCase::new(
            "ORM-CATALOG-01",
            vec![
                catalog(CatalogOperation::ListTables),
                document(&query(
                    "people",
                    [Expr::aggregate("COUNT", [], false, None, vec![])],
                )),
            ],
        ),
        FixtureCase::new(
            "ORM-CATALOG-02",
            vec![catalog(CatalogOperation::DescribeTable {
                table: "people".to_string(),
            })],
        ),
        FixtureCase::new(
            "ORM-DDL-01",
            vec![document(
                &DdlBuilder::create_table("fio_copy")
                    .column(
                        Column::uuid("id")
                            .not_null(true)
                            .primary_key(true)
                            .unique(false),
                    )
                    .column(
                        Column::text("name")
                            .not_null(false)
                            .primary_key(false)
                            .unique(false),
                    ),
            )],
        ),
        FixtureCase::new(
            "ORM-DDL-02",
            vec![document(
                &DdlBuilder::create_table("all_types")
                    .column(Column::integer("integer_value"))
                    .column(Column::float("float_value"))
                    .column(Column::decimal("decimal_value", 38, 10))
                    .column(Column::text("text_value"))
                    .column(Column::boolean("boolean_value"))
                    .column(Column::timestamp("timestamp_value"))
                    .column(Column::date("date_value"))
                    .column(Column::json("json_value"))
                    .column(Column::uuid("uuid_value"))
                    .column(Column::bytes("bytes_value"))
                    .column(Column::vector("embedding", 768)),
            )],
        ),
        FixtureCase::new(
            "ORM-DDL-03",
            vec![
                document(
                    &DdlBuilder::alter_table("people")
                        .add_constraint(check(Expr::column("email").ne(""))),
                ),
                document(&DdlBuilder::alter_table("people").drop_constraint("chk_people_1", false)),
            ],
        ),
        FixtureCase::new(
            "ORM-DDL-03B",
            vec![document(
                &DdlBuilder::create_table("constraint_collision")
                    .column(Column::integer("a_b"))
                    .column(Column::integer("c"))
                    .column(Column::integer("a"))
                    .column(Column::integer("b_c"))
                    .constraint(unique(["a_b", "c"]))
                    .constraint(unique(["a", "b_c"])),
            )],
        ),
        FixtureCase::new(
            "ORM-DDL-04",
            vec![
                transaction(TransactionOperation::Begin),
                document(
                    &DdlBuilder::alter_table("people").add_column(Column::text("display_name")),
                ),
                document(
                    &DdlBuilder::alter_table("people")
                        .add_column(Column::integer("revision").default(0_i64)),
                ),
                document(
                    &DdlBuilder::alter_table("people")
                        .add_constraint(check(Expr::column("revision").gte(0_i64))),
                ),
                transaction(TransactionOperation::Commit),
            ],
        ),
        FixtureCase::new(
            "ORM-DDL-05",
            vec![document(&DdlBuilder::create_index(
                IndexDefinition::new("uq_people_live_email", "people", ["email"])
                    .unique(true)
                    .where_(Expr::column("deleted_at").is_null()),
            ))],
        ),
        FixtureCase::new(
            "ORM-REF-01",
            vec![document(&insert_person(
                uuid_a,
                "user@example.test",
                Some(uuid_b),
            ))],
        ),
        FixtureCase::new(
            "ORM-REF-02",
            vec![
                transaction(TransactionOperation::Begin),
                document(
                    &InsertBuilder::new("fio")
                        .value("id", TypedValue::Uuid(uuid_b.to_string()))
                        .value("name", "Иванов Иван Иванович")
                        .returning_all(),
                ),
                document(&insert_person(uuid_a, "user@example.test", Some(uuid_b))),
                transaction(TransactionOperation::Commit),
            ],
        ),
        FixtureCase::new(
            "ORM-REF-03",
            vec![document(
                &UpdateBuilder::new("people")
                    .set("fio", TypedValue::Null(DataTypeDescriptor::Uuid))
                    .filter(Expr::column("id").eq(TypedValue::Uuid(uuid_a.to_string())))
                    .returning_all(),
            )],
        ),
        FixtureCase::new(
            "ORM-DML-01",
            vec![document(&insert_person(
                uuid_a,
                "strict@example.test",
                None,
            ))],
        ),
        FixtureCase::new(
            "ORM-DML-02",
            vec![document(
                &insert_person(uuid_a, "saved@example.test", Some(uuid_b))
                    .on_conflict(["id"])
                    .do_update("email", Expr::qualified("excluded", "email"))
                    .do_update("fio", Expr::qualified("excluded", "fio")),
            )],
        ),
        FixtureCase::new(
            "ORM-DML-03",
            vec![document(
                &InsertBuilder::new("payroll_documents")
                    .value("id", TypedValue::Uuid(uuid_a.to_string()))
                    .value("employee", TypedValue::Uuid(uuid_b.to_string()))
                    .value("period", TypedValue::Date("2026-01-01".to_string()))
                    .value("amount", TypedValue::Decimal("100.00".to_string()))
                    .value("status", "paid")
                    .on_conflict(["employee", "period"])
                    .do_update("amount", Expr::qualified("excluded", "amount"))
                    .do_update("status", Expr::qualified("excluded", "status"))
                    .returning_all(),
            )],
        ),
        FixtureCase::new(
            "ORM-DML-04",
            vec![document(
                &UpdateBuilder::new("people")
                    .set("email", "new@example.test")
                    .filter(Expr::column("id").eq(TypedValue::Uuid(uuid_a.to_string())))
                    .returning_all(),
            )],
        ),
        FixtureCase::new(
            "ORM-DML-05",
            vec![document(
                &UpdateBuilder::new("payroll_documents")
                    .set(
                        "amount",
                        Expr::column("amount").mul(TypedValue::Decimal("1.10".to_string())),
                    )
                    .set("status", "recalculated")
                    .filter(
                        Expr::column("period")
                            .between(
                                TypedValue::Date("2026-01-01".to_string()),
                                TypedValue::Date("2026-12-31".to_string()),
                            )
                            .and(Expr::column("status").in_list(["draft", "approved"])),
                    ),
            )],
        ),
        FixtureCase::new(
            "ORM-DML-06",
            vec![document(
                &DeleteBuilder::new("people")
                    .filter(
                        Expr::column("deleted_at").is_not_null().and(
                            Expr::column("deleted_at")
                                .lt(TypedValue::Timestamp("2026-01-01T00:00:00Z".to_string())),
                        ),
                    )
                    .returning([Expr::column("id")]),
            )],
        ),
    ];

    let complex_predicate = Expr::column("active")
        .eq(true)
        .and(
            Expr::column("hired_at")
                .gte(TypedValue::Date("2020-01-01".to_string()))
                .or(Expr::column("hired_at").is_null()),
        )
        .and(
            Expr::column("email")
                .like("%@example.test")
                .or(Expr::column("email").regexp("^[a-z]+@corp\\.test$")),
        )
        .and(Expr::column("deleted_at").is_null())
        .and(Expr::column("position").in_list([uuid_a, uuid_b]).not());
    cases.push(FixtureCase::new(
        "ORM-WHERE-01",
        vec![document(
            &query("people", [Expr::column("id"), Expr::column("email")])
                .filter(complex_predicate)
                .order_by([Expr::column("email").asc(), Expr::column("id").asc()])
                .limit(100),
        )],
    ));
    cases.push(FixtureCase::new(
        "ORM-WHERE-02",
        vec![document(
            &QueryBuilder::from_relation(table("payroll_documents")).select_projections(vec![
                Expr::column("id").projection(),
                Expr::case(
                    None,
                    [
                        (
                            Expr::column("amount")
                                .gte(TypedValue::Decimal("100000.00".to_string())),
                            Expr::from("large"),
                        ),
                        (
                            Expr::column("amount").gte(TypedValue::Decimal("10000.00".to_string())),
                            Expr::from("medium"),
                        ),
                    ],
                    Some(Expr::from("small")),
                )
                .alias("bucket"),
                Expr::column("amount")
                    .mul(TypedValue::Decimal("1.13".to_string()))
                    .cast(DataTypeDescriptor::Text)
                    .alias("gross_text"),
            ]),
        )],
    ));

    cases.extend(join_cases());
    cases.extend(navigation_cases());
    cases.extend(aggregate_cases());
    cases.extend(relation_cases());

    cases.push(FixtureCase::new(
        "ORM-JSON-01",
        vec![document(
            &DdlBuilder::alter_table("people").drop_constraint("uq_people_email", false),
        )],
    ));
    cases.push(FixtureCase::new(
        "ORM-JSON-02",
        vec![document(
            &query("people", [Expr::column("id"), Expr::column("email")])
                .filter(Expr::column("id").eq(TypedValue::Uuid(uuid_a.to_string()))),
        )],
    ));
    cases.push(
        FixtureCase::new(
            "ORM-JSON-03",
            vec![document(&query("people", [Expr::column("id")]))],
        )
        .rejected(vec![
            json!({"ir":"radixdb.orm.v2","kind":"select","payload":{}}),
            json!({"ir":"radixdb.orm.v1","kind":"select","payload":{"node":"raw_sql","sql":"SELECT 1"}}),
            json!({"ir":"radixdb.orm.v1","kind":"select","payload":{"node":"select","query":{"projection":[{"expression":{"node":"literal","value":7},"alias":null}]}}}),
        ]),
    );
    cases.push(FixtureCase::new(
        "ORM-MIXED-01",
        vec![
            transaction(TransactionOperation::Begin),
            document(&insert_person(uuid_a, "mixed@example.test", Some(uuid_b))),
            document(
                &query(
                    "people",
                    [Expr::aggregate("COUNT", [], false, None, vec![])],
                )
                .filter(Expr::column("fio").eq(TypedValue::Uuid(uuid_b.to_string()))),
            ),
            transaction(TransactionOperation::Commit),
        ],
    ));

    cases.sort_by_key(|case| case.id);
    let artifact = FixtureArtifact {
        fixtures: "radixdb.orm.fixtures.v1",
        cases,
    };
    println!("{}", serde_json::to_string_pretty(&artifact).unwrap());
}

fn join_cases() -> Vec<FixtureCase> {
    let p_id = Expr::qualified("p", "id");
    let p_fio = Expr::qualified("p", "fio");
    let f_id = Expr::qualified("f", "id");
    let f_name = Expr::qualified("f", "name");
    let inner = QueryBuilder::from_relation(table_as("people", "p"))
        .inner_join(table_as("fio", "f"), p_fio.clone().eq(f_id.clone()))
        .select([p_id.clone(), Expr::qualified("p", "email"), f_name.clone()]);
    let left = QueryBuilder::from_relation(table_as("people", "p"))
        .left_join(
            table_as("fio", "f"),
            p_fio
                .clone()
                .eq(f_id.clone())
                .and(f_name.clone().is_not_null()),
        )
        .select([p_id.clone(), f_name]);
    let chain = QueryBuilder::from_relation(table_as("payroll_documents", "d"))
        .inner_join(
            table_as("people", "p"),
            Expr::qualified("d", "employee").eq(p_id.clone()),
        )
        .left_join(
            table_as("positions", "pos"),
            Expr::qualified("p", "position").eq(Expr::qualified("pos", "id")),
        )
        .left_join(
            table_as("departments", "dep"),
            Expr::qualified("pos", "department").eq(Expr::qualified("dep", "id")),
        )
        .select([
            Expr::qualified("d", "id"),
            Expr::qualified("p", "email"),
            Expr::qualified("pos", "name"),
            Expr::qualified("dep", "name"),
        ]);
    let right = QueryBuilder::from_relation(table_as("people", "p"))
        .right_join(table_as("fio", "f"), p_id.clone().eq(f_id.clone()))
        .select([p_id.clone(), f_id.clone()]);
    let full = QueryBuilder::from_relation(table_as("people", "p"))
        .full_join(table_as("fio", "f"), p_id.clone().eq(f_id.clone()))
        .select([p_id.clone(), f_id.clone()]);
    let cross = QueryBuilder::from_relation(table_as("people", "p"))
        .cross_join(table_as("fio", "f"))
        .select([p_id, f_id]);
    vec![
        FixtureCase::new("ORM-JOIN-01", vec![document(&inner)]),
        FixtureCase::new("ORM-JOIN-02", vec![document(&left)]),
        FixtureCase::new("ORM-JOIN-03", vec![document(&chain)]),
        FixtureCase::new(
            "ORM-JOIN-04",
            vec![document(&right), document(&full), document(&cross)],
        ),
    ]
}

fn navigation_cases() -> Vec<FixtureCase> {
    let direct = query(
        "people",
        [
            Expr::column("id"),
            Expr::column("email"),
            Expr::navigation("people", ["fio", "name"]),
        ],
    );
    let shared = query(
        "people",
        [
            Expr::navigation("people", ["fio", "name"]),
            Expr::navigation("people", ["fio", "short_name"]),
        ],
    );
    let transitive = query(
        "people",
        [
            Expr::column("email"),
            Expr::navigation("people", ["position", "name"]),
            Expr::navigation("people", ["position", "department", "name"]),
        ],
    );
    let filtered = select_star("people").filter(
        Expr::navigation("people", ["position", "department", "active"])
            .eq(true)
            .or(Expr::column("position").is_null()),
    );
    let classic = QueryBuilder::from_relation(table_as("people", "p"))
        .left_join(
            table_as("fio", "f"),
            Expr::qualified("p", "fio").eq(Expr::qualified("f", "id")),
        )
        .select([Expr::qualified("p", "id"), Expr::qualified("f", "name")]);
    vec![
        FixtureCase::new("ORM-NAV-01", vec![document(&direct)]),
        FixtureCase::new("ORM-NAV-02", vec![document(&shared)]),
        FixtureCase::new("ORM-NAV-03", vec![document(&transitive)]),
        FixtureCase::new("ORM-NAV-04", vec![document(&filtered)]),
        FixtureCase::new("ORM-NAV-05", vec![document(&direct), document(&classic)]),
    ]
}

fn aggregate_cases() -> Vec<FixtureCase> {
    let amount = Expr::column("amount");
    let status = Expr::column("status");
    let period = Expr::column("period");
    let total = Expr::aggregate("SUM", [amount.clone()], false, None, vec![]);
    let department = Expr::navigation(
        "payroll_documents",
        ["employee", "position", "department", "name"],
    );
    let nav_group = QueryBuilder::from_relation(table("payroll_documents"))
        .select_projections(vec![
            department.clone().alias("department"),
            Expr::aggregate("COUNT", [], false, None, vec![]).alias("documents"),
            total.clone().alias("total"),
        ])
        .filter(status.clone().ne("cancelled"))
        .group_by(Grouping::Expressions {
            expressions: vec![department.0],
        })
        .having(
            total
                .clone()
                .gt(TypedValue::Decimal("100000.00".to_string())),
        )
        .order_by([total.clone().desc()]);
    let ordered = query(
        "payroll_documents",
        [
            Expr::aggregate("COUNT", [Expr::column("employee")], true, None, vec![]),
            Expr::aggregate(
                "SUM",
                [amount.clone()],
                false,
                Some(status.clone().eq("paid")),
                vec![],
            ),
            Expr::aggregate(
                "ARRAY_AGG",
                [amount.clone()],
                false,
                None,
                vec![period.clone().asc()],
            ),
            Expr::aggregate(
                "STRING_AGG",
                [status.clone(), Expr::from("|")],
                true,
                None,
                vec![status.clone().asc()],
            ),
        ],
    );
    let grouped = |grouping| {
        query(
            "payroll_documents",
            [status.clone(), period.clone(), total.clone()],
        )
        .group_by(grouping)
    };
    let rollup = grouped(Grouping::Rollup {
        expressions: vec![status.clone().0, period.clone().0],
    });
    let cube = grouped(Grouping::Cube {
        expressions: vec![status.clone().0, period.clone().0],
    });
    let sets = grouped(Grouping::Sets {
        sets: vec![
            vec![status.clone().0, period.clone().0],
            vec![status.clone().0],
            vec![period.clone().0],
            vec![],
        ],
    });
    let window = query(
        "payroll_documents",
        [
            Expr::column("id"),
            Expr::column("employee"),
            amount.clone(),
            Expr::function("ROW_NUMBER", []).window(WindowSpecification {
                name: None,
                partition_by: vec![Expr::column("employee").0],
                order_by: vec![amount.clone().desc().nulls_last().0],
                frame: None,
            }),
            total.window(WindowSpecification {
                name: None,
                partition_by: vec![Expr::column("employee").0],
                order_by: vec![period.asc().0],
                frame: Some(WindowFrame {
                    unit: WindowFrameUnit::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::CurrentRow),
                }),
            }),
        ],
    );
    vec![
        FixtureCase::new("ORM-AGG-01", vec![document(&nav_group)]),
        FixtureCase::new("ORM-AGG-02", vec![document(&ordered)]),
        FixtureCase::new("ORM-AGG-03", vec![document(&rollup)]),
        FixtureCase::new("ORM-AGG-04", vec![document(&cube)]),
        FixtureCase::new("ORM-AGG-05", vec![document(&sets)]),
        FixtureCase::new("ORM-WINDOW-01", vec![document(&window)]),
    ]
}

fn relation_cases() -> Vec<FixtureCase> {
    let paid = query(
        "payroll_documents",
        [Expr::column("employee"), Expr::column("amount")],
    )
    .filter(Expr::column("status").eq("paid"));
    let cte_query = QueryBuilder::from_relation(cte("paid"))
        .select([
            Expr::column("employee"),
            Expr::aggregate("SUM", [Expr::column("amount")], false, None, vec![]),
        ])
        .group_by(Grouping::Expressions {
            expressions: vec![Expr::column("employee").0],
        })
        .with_cte(CommonTableExpression {
            name: "paid".to_string(),
            columns: vec!["employee".to_string(), "amount".to_string()],
            query: Box::new(paid.into_select()),
        });
    let correlated = select_star("people").filter(Expr::exists(
        query("payroll_documents", [Expr::from(1_i64)]).filter(
            Expr::qualified("payroll_documents", "employee")
                .eq(Expr::qualified("people", "id"))
                .and(
                    Expr::qualified("payroll_documents", "amount")
                        .gt(TypedValue::Decimal("100000.00".to_string())),
                ),
        ),
    ));
    let live = query("people", [Expr::column("id")]).filter(Expr::column("deleted_at").is_null());
    let payroll = query("payroll_documents", [Expr::column("employee")]);
    let union = live
        .clone()
        .set_operation(SetOperator::Union, payroll.clone());
    let intersect = live
        .clone()
        .set_operation(SetOperator::Intersect, payroll.clone());
    let except = live.set_operation(SetOperator::Except, payroll);
    vec![
        FixtureCase::new("ORM-REL-01", vec![document(&cte_query)]),
        FixtureCase::new("ORM-REL-02", vec![document(&correlated)]),
        FixtureCase::new(
            "ORM-REL-03",
            vec![document(&union), document(&intersect), document(&except)],
        ),
    ]
}
