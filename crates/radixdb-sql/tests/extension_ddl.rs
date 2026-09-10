use radixdb_sql::{ast::Statement, parse_sql};

#[test]
fn extension_binding_ddl_roundtrips_canonically() {
    let create = parse_sql("CREATE EXTENSION IF NOT EXISTS radix_spatial VERSION '1.2.3'")
        .expect("parse CREATE EXTENSION");
    let Statement::CreateExtension(create) = &create[0] else {
        panic!("expected CREATE EXTENSION AST");
    };
    assert_eq!(create.name.value, "radix_spatial");
    assert_eq!(create.version, "1.2.3");
    assert!(create.if_not_exists);
    assert_eq!(
        create.to_string(),
        "CREATE EXTENSION IF NOT EXISTS radix_spatial VERSION '1.2.3'"
    );

    let drop =
        parse_sql("DROP EXTENSION IF EXISTS radix_spatial RESTRICT").expect("parse DROP EXTENSION");
    let Statement::DropExtension(drop) = &drop[0] else {
        panic!("expected DROP EXTENSION AST");
    };
    assert_eq!(drop.name.value, "radix_spatial");
    assert!(drop.if_exists);
    assert_eq!(
        drop.to_string(),
        "DROP EXTENSION IF EXISTS radix_spatial RESTRICT"
    );
}

#[test]
fn extension_ddl_rejects_implicit_version_and_cascade() {
    assert!(parse_sql("CREATE EXTENSION radix_spatial").is_err());
    assert!(parse_sql("DROP EXTENSION radix_spatial").is_err());
    assert!(parse_sql("DROP EXTENSION radix_spatial CASCADE").is_err());
    assert!(parse_sql("CREATE EXTENSION radix_spatial VERSION 1").is_err());
}

#[test]
fn operator_and_operator_class_ddl_roundtrip_canonically() {
    let create_operator = parse_sql(
        "CREATE OPERATOR geo.&& (LEFTARG = geo.box, RIGHTARG = geo.box, \
         FUNCTION = geo.box_overlap(geo.box, geo.box)) \
         FROM EXTENSION radix_spatial AS 'box_overlap_operator'",
    )
    .expect("parse CREATE OPERATOR");
    let Statement::CreateOperator(create_operator) = &create_operator[0] else {
        panic!("expected CREATE OPERATOR AST");
    };
    assert_eq!(create_operator.name.schema.value, "geo");
    assert_eq!(create_operator.name.symbol.as_str(), "&&");
    assert_eq!(
        create_operator.to_string(),
        "CREATE OPERATOR geo.&& (LEFTARG = geo.box, RIGHTARG = geo.box, FUNCTION = geo.box_overlap(geo.box, geo.box)) FROM EXTENSION radix_spatial AS 'box_overlap_operator'"
    );

    let drop_operator = parse_sql("DROP OPERATOR IF EXISTS geo.&& (geo.box, geo.box) RESTRICT")
        .expect("parse DROP OPERATOR");
    let Statement::DropOperator(drop_operator) = &drop_operator[0] else {
        panic!("expected DROP OPERATOR AST");
    };
    assert!(drop_operator.if_exists);
    assert_eq!(
        drop_operator.to_string(),
        "DROP OPERATOR IF EXISTS geo.&& (geo.box, geo.box) RESTRICT"
    );

    let create_class = parse_sql(
        "CREATE OPERATOR CLASS geo.point_btree FOR TYPE geo.point USING BTREE \
         FROM EXTENSION radix_spatial AS 'point_btree'",
    )
    .expect("parse CREATE OPERATOR CLASS");
    let Statement::CreateOperatorClass(create_class) = &create_class[0] else {
        panic!("expected CREATE OPERATOR CLASS AST");
    };
    assert_eq!(create_class.to_string(), "CREATE OPERATOR CLASS geo.point_btree FOR TYPE geo.point USING BTREE FROM EXTENSION radix_spatial AS 'point_btree'");

    let drop_class =
        parse_sql("DROP OPERATOR CLASS IF EXISTS geo.point_btree USING BTREE RESTRICT")
            .expect("parse DROP OPERATOR CLASS");
    let Statement::DropOperatorClass(drop_class) = &drop_class[0] else {
        panic!("expected DROP OPERATOR CLASS AST");
    };
    assert!(drop_class.if_exists);
    assert_eq!(
        drop_class.to_string(),
        "DROP OPERATOR CLASS IF EXISTS geo.point_btree USING BTREE RESTRICT"
    );
}

#[test]
fn planner_support_ddl_roundtrips_canonically() {
    let create = parse_sql(
        "CREATE PLANNER SUPPORT geo.dwithin_support FOR FUNCTION \
         geo.st_dwithin(geo.point, geo.point, FLOAT) \
         FROM EXTENSION radix_spatial AS 'dwithin_support'",
    )
    .expect("parse CREATE PLANNER SUPPORT");
    let Statement::CreatePlannerSupport(create) = &create[0] else {
        panic!("expected CREATE PLANNER SUPPORT AST");
    };
    assert_eq!(
        create.to_string(),
        "CREATE PLANNER SUPPORT geo.dwithin_support FOR FUNCTION geo.st_dwithin(geo.point, geo.point, FLOAT) FROM EXTENSION radix_spatial AS 'dwithin_support'"
    );

    let drop = parse_sql("DROP PLANNER SUPPORT IF EXISTS geo.dwithin_support RESTRICT")
        .expect("parse DROP PLANNER SUPPORT");
    let Statement::DropPlannerSupport(drop) = &drop[0] else {
        panic!("expected DROP PLANNER SUPPORT AST");
    };
    assert_eq!(
        drop.to_string(),
        "DROP PLANNER SUPPORT IF EXISTS geo.dwithin_support RESTRICT"
    );
}

#[test]
fn index_operator_class_and_custom_infix_operator_parse() {
    let statements = parse_sql(
        "CREATE INDEX points_cell ON points(point geo.point_btree) USING BTREE; \
         SELECT id FROM points WHERE point && $1",
    )
    .expect("parse operator-class index and custom infix operator");
    let Statement::CreateIndex(index) = &statements[0] else {
        panic!("expected CREATE INDEX AST");
    };
    assert_eq!(
        index.operator_class.as_ref().map(ToString::to_string),
        Some("geo.point_btree".to_owned())
    );
    assert_eq!(
        index.to_string(),
        "CREATE INDEX points_cell ON points (point geo.point_btree) USING BTREE"
    );
    assert!(matches!(statements[1], Statement::Select(_)));
}

#[test]
fn operator_ddl_rejects_implicit_names_and_destructive_forms() {
    assert!(parse_sql(
        "CREATE OPERATOR && (LEFTARG = geo.box, RIGHTARG = geo.box, FUNCTION = geo.box_overlap(geo.box, geo.box)) FROM EXTENSION radix_spatial AS 'overlap'"
    )
    .is_err());
    assert!(parse_sql(
        "CREATE OPERATOR CLASS point_btree FOR TYPE geo.point USING BTREE FROM EXTENSION radix_spatial AS 'point_btree'"
    )
    .is_err());
    assert!(parse_sql("DROP OPERATOR geo.&& (geo.box, geo.box) CASCADE").is_err());
    assert!(parse_sql("DROP OPERATOR CLASS geo.point_btree USING BTREE CASCADE").is_err());
    assert!(parse_sql("CREATE INDEX invalid ON points(a, b geo.point_btree) USING BTREE").is_err());
}
