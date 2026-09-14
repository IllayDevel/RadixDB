use radixdb_orm::*;

#[test]
fn case_and_function_composition_preserve_values_and_ordering() {
    let expression = Expr::case(
        Some(Expr::column("kind")),
        [(
            Expr::value("raw"),
            Expr::function("LOWER", [Expr::column("name")]),
        )],
        Some(Expr::value("unknown")),
    );
    let query = QueryBuilder::from_relation(table("items"))
        .select_projections(vec![expression.alias("label")])
        .order_by([
            Expr::column("name").asc().nulls_last(),
            Expr::column("id").desc().nulls_first(),
        ]);
    let statement = query.to_sql().unwrap();
    assert_eq!(statement.sql, "SELECT CASE \"kind\" WHEN $1 THEN \"LOWER\"(\"name\") ELSE $2 END AS \"label\" FROM \"items\" ORDER BY \"name\" ASC NULLS LAST, \"id\" DESC NULLS FIRST");
    assert_eq!(
        statement.parameters,
        vec![
            TypedValue::Text("raw".into()),
            TypedValue::Text("unknown".into())
        ]
    );
    assert_eq!(
        IrDocument::from_json(&query.to_json().unwrap()).unwrap(),
        query.document().unwrap()
    );

    let tuple = compile(Expr::tuple([Expr::value(2_i64), Expr::value(3_i64)]));
    assert_eq!(tuple.sql, "SELECT ($1, $2) FROM \"items\"");
    assert_eq!(
        tuple.parameters,
        vec![TypedValue::Integer(2), TypedValue::Integer(3)]
    );
    let negated = compile(Expr::value(true).not());
    assert_eq!(negated.sql, "SELECT NOT ($1) FROM \"items\"");
    assert_eq!(negated.parameters, vec![TypedValue::Boolean(true)]);
}

fn compile(expression: Expr) -> CompiledStatement {
    let query = QueryBuilder::from_relation(table("items")).select([expression]);
    let document = query.document().unwrap();
    assert_eq!(
        IrDocument::from_json(&query.to_json().unwrap()).unwrap(),
        document
    );
    query.to_sql().unwrap()
}

#[test]
fn fluent_binary_operators_preserve_ir_sql_and_bound_values() {
    let left = Expr::column("value");
    for (expression, operator, spelling, value) in [
        (
            left.clone().eq(7_i64),
            BinaryOperator::Eq,
            "=",
            TypedValue::Integer(7),
        ),
        (
            left.clone().ne(7_i64),
            BinaryOperator::Ne,
            "<>",
            TypedValue::Integer(7),
        ),
        (
            left.clone().lt(7_i64),
            BinaryOperator::Lt,
            "<",
            TypedValue::Integer(7),
        ),
        (
            left.clone().lte(7_i64),
            BinaryOperator::Lte,
            "<=",
            TypedValue::Integer(7),
        ),
        (
            left.clone().gt(7_i64),
            BinaryOperator::Gt,
            ">",
            TypedValue::Integer(7),
        ),
        (
            left.clone().gte(7_i64),
            BinaryOperator::Gte,
            ">=",
            TypedValue::Integer(7),
        ),
        (
            left.clone().add(7_i64),
            BinaryOperator::Add,
            "+",
            TypedValue::Integer(7),
        ),
        (
            left.clone().sub(7_i64),
            BinaryOperator::Subtract,
            "-",
            TypedValue::Integer(7),
        ),
        (
            left.clone().mul(7_i64),
            BinaryOperator::Multiply,
            "*",
            TypedValue::Integer(7),
        ),
        (
            left.clone().div(7_i64),
            BinaryOperator::Divide,
            "/",
            TypedValue::Integer(7),
        ),
        (
            left.clone().modulo(7_i64),
            BinaryOperator::Modulo,
            "%",
            TypedValue::Integer(7),
        ),
        (
            left.clone().and(true),
            BinaryOperator::And,
            "AND",
            TypedValue::Boolean(true),
        ),
        (
            left.clone().or(false),
            BinaryOperator::Or,
            "OR",
            TypedValue::Boolean(false),
        ),
        (
            left.clone().like("a%"),
            BinaryOperator::Like,
            "LIKE",
            TypedValue::Text("a%".into()),
        ),
        (
            left.clone().not_like("a%"),
            BinaryOperator::NotLike,
            "NOT LIKE",
            TypedValue::Text("a%".into()),
        ),
        (
            left.clone().regexp("^a"),
            BinaryOperator::Regexp,
            "REGEXP",
            TypedValue::Text("^a".into()),
        ),
        (
            left.clone().glob("a*"),
            BinaryOperator::Glob,
            "GLOB",
            TypedValue::Text("a*".into()),
        ),
        (
            left.clone().is_distinct_from(7_i64),
            BinaryOperator::IsDistinctFrom,
            "IS DISTINCT FROM",
            TypedValue::Integer(7),
        ),
        (
            left.clone().is_not_distinct_from(7_i64),
            BinaryOperator::IsNotDistinctFrom,
            "IS NOT DISTINCT FROM",
            TypedValue::Integer(7),
        ),
    ] {
        assert_eq!(
            expression.0,
            Expression::Binary {
                left: Box::new(left.0.clone()),
                operator,
                right: Box::new(Expression::literal(value.clone())),
            }
        );
        let statement = compile(expression);
        assert_eq!(
            statement.sql,
            format!("SELECT (\"value\" {spelling} $1) FROM \"items\"")
        );
        assert_eq!(statement.parameters, vec![value]);
    }
}

#[test]
fn negated_predicates_preserve_parameter_order() {
    let column = Expr::column("value");
    for (expression, predicate, values) in [
        (column.clone().is_null(), "(\"value\" IS NULL)", vec![]),
        (
            column.clone().is_not_null(),
            "(\"value\" IS NOT NULL)",
            vec![],
        ),
        (
            column.clone().between(2_i64, 9_i64),
            "(\"value\" BETWEEN $1 AND $2)",
            vec![2, 9],
        ),
        (
            column.clone().not_between(2_i64, 9_i64),
            "(\"value\" NOT BETWEEN $1 AND $2)",
            vec![2, 9],
        ),
        (
            column.clone().in_list([2_i64, 9_i64]),
            "(\"value\" IN ($1, $2))",
            vec![2, 9],
        ),
        (
            column.not_in_list([2_i64, 9_i64]),
            "(\"value\" NOT IN ($1, $2))",
            vec![2, 9],
        ),
    ] {
        let statement = compile(expression);
        assert_eq!(statement.sql, format!("SELECT {predicate} FROM \"items\""));
        assert_eq!(
            statement.parameters,
            values
                .into_iter()
                .map(TypedValue::Integer)
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn nested_queries_keep_one_parameter_namespace() {
    let subquery = QueryBuilder::from_relation(table("other"))
        .select([Expr::column("id")])
        .filter(Expr::column("rank").gt(3_i64));
    for (expression, fragment) in [
        (
            Expr::column("id").in_subquery(subquery.clone()),
            " IN (SELECT",
        ),
        (
            Expr::column("id").not_in_subquery(subquery.clone()),
            " NOT IN (SELECT",
        ),
        (Expr::exists(subquery.clone()), "EXISTS (SELECT"),
        (Expr::not_exists(subquery.clone()), "NOT EXISTS (SELECT"),
        (Expr::scalar_subquery(subquery), "(SELECT"),
    ] {
        let query =
            QueryBuilder::from_relation(table("items")).select([Expr::value(11_i64), expression]);
        let statement = query.to_sql().unwrap();
        assert!(statement.sql.contains(fragment), "{}", statement.sql);
        assert!(statement.sql.contains("\"rank\" > $2"));
        assert_eq!(
            statement.parameters,
            vec![TypedValue::Integer(11), TypedValue::Integer(3)]
        );
        assert_eq!(
            IrDocument::from_json(&query.to_json().unwrap()).unwrap(),
            query.document().unwrap()
        );
    }
}
