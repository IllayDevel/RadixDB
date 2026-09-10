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

use super::*;

fn parse_stmt(input: &str) -> Option<Statement> {
    let mut parser = Parser::new(input);
    parser.parse_statement()
}

#[test]
fn test_parse_simple_select() {
    let stmt = parse_stmt("SELECT * FROM users").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            assert!(matches!(select.columns[0], Expression::Star(_)));
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_with_where() {
    let stmt = parse_stmt("SELECT id, name FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.columns.len(), 2);
            assert!(select.where_clause.is_some());
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn test_parse_count_star_with_filter_in_select() {
    let stmt = parse_stmt("SELECT COUNT(*) FILTER (WHERE category = 'Z') FROM data").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.columns.len(), 1);
            match &select.columns[0] {
                Expression::FunctionCall(fc) => {
                    assert_eq!(fc.function.to_uppercase(), "COUNT");
                    assert!(
                        fc.filter.is_some(),
                        "FILTER clause should be parsed for COUNT(*) in SELECT"
                    );
                }
                _ => panic!("expected FunctionCall for COUNT(*)"),
            }
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_with_join() {
    let stmt =
        parse_stmt("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.user_id").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.table_expr.is_some());
            match select.table_expr.as_ref().unwrap().as_ref() {
                Expression::JoinSource(_) => {}
                _ => panic!("expected JoinSource"),
            }
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn table_sources_accept_new_non_reserved_keywords_as_aliases() {
    for source in [
        "SELECT owner.id FROM users owner INNER JOIN groups admin ON admin.id = owner.id",
        "SELECT owner.id FROM users AS owner",
        "SELECT owner.id FROM (SELECT id FROM users) owner",
        "SELECT owner.id FROM (VALUES (1)) owner(id)",
        "SELECT owner.value FROM generate_series(1, 2) owner(value)",
    ] {
        crate::parse_sql(source)
            .unwrap_or_else(|error| panic!("failed to parse keyword alias in {source:?}: {error}"));
    }
}

#[test]
fn dml_accepts_new_non_reserved_keywords_as_relation_names() {
    for source in [
        "INSERT INTO schedule VALUES (1)",
        "UPDATE schedule SET value = 1",
        "DELETE FROM schedule",
        "DELETE FROM users owner WHERE owner.id = 1",
        "TRUNCATE schedule",
        "VACUUM schedule",
    ] {
        crate::parse_sql(source).unwrap_or_else(|error| {
            panic!("failed to parse keyword relation name in {source:?}: {error}")
        });
    }
}

#[test]
fn test_parse_insert() {
    let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name.value, "users");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.values.len(), 1);
        }
        _ => panic!("expected InsertStatement"),
    }
}

#[test]
fn test_parse_update() {
    let stmt = parse_stmt("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
    match stmt {
        Statement::Update(update) => {
            assert_eq!(update.table_name.value, "users");
            assert_eq!(update.updates.len(), 1);
            assert!(update.where_clause.is_some());
        }
        _ => panic!("expected UpdateStatement"),
    }
}

#[test]
fn write_targets_reject_navigation_syntax_with_stable_code() {
    for sql in [
        "UPDATE users SET profile.name = 'Bob' WHERE id = 1",
        "INSERT INTO users (id, profile.name) VALUES (1, 'Bob')",
        "INSERT INTO users (id, name) VALUES (1, 'Bob') \
         ON DUPLICATE KEY UPDATE profile.name = 'Robert'",
        "INSERT INTO users (id, name) VALUES (1, 'Bob') \
         ON CONFLICT (profile.name) DO NOTHING",
    ] {
        let mut parser = Parser::new(sql);
        let _ = parser.parse_statement();
        let errors = parser
            .errors()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(errors.contains("NAVIGATION_READ_ONLY"), "{sql}\n{errors}");
    }
}

#[test]
fn test_parse_delete() {
    let stmt = parse_stmt("DELETE FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Delete(delete) => {
            assert_eq!(delete.table_name.value, "users");
            assert!(delete.where_clause.is_some());
        }
        _ => panic!("expected DeleteStatement"),
    }
}

#[test]
fn test_parse_create_table() {
    let stmt =
        parse_stmt("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    match stmt {
        Statement::CreateTable(create) => {
            assert_eq!(create.table_name.value, "users");
            assert_eq!(create.columns.len(), 2);
        }
        _ => panic!("expected CreateTableStatement"),
    }
}

#[test]
fn test_parse_create_table_with_parameterized_type_aliases() {
    let stmt = parse_stmt(
        "CREATE TABLE typed (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            amount DECIMAL(10, 2),
            score NUMERIC(12),
            embedding VECTOR(3),
            payload JSONB,
            bytes VARBINARY
        )",
    )
    .unwrap();

    match stmt {
        Statement::CreateTable(create) => {
            let data_types: Vec<&str> = create
                .columns
                .iter()
                .map(|column| column.data_type.as_str())
                .collect();
            assert_eq!(
                data_types,
                vec![
                    "INT",
                    "VARCHAR(255)",
                    "DECIMAL(10,2)",
                    "NUMERIC(12)",
                    "VECTOR(3)",
                    "JSONB",
                    "VARBINARY"
                ]
            );
        }
        _ => panic!("expected CreateTableStatement"),
    }
}

#[test]
fn test_parse_create_table_rejects_malformed_type_arguments() {
    let mut parser = Parser::new("CREATE TABLE bad_vector (embedding VECTOR(name))");
    assert!(parser.parse_program().is_err());

    let mut parser = Parser::new("CREATE TABLE bad_trailing (name VARCHAR(255,))");
    assert!(parser.parse_program().is_err());

    let mut parser = Parser::new("CREATE TABLE bad_empty (name VARCHAR())");
    assert!(parser.parse_program().is_err());
}

#[test]
fn r4_l05_messenger_contracts_parse_alter_table_create_equivalent_constraints() {
    let statements = [
        "ALTER TABLE children ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE",
        "ALTER TABLE children ADD FOREIGN KEY (parent_id) REFERENCES parents(id)",
        "ALTER TABLE children ADD CONSTRAINT UNIQUE(parent_id)",
        "ALTER TABLE children ADD CONSTRAINT CHECK(parent_id > 0)",
        "ALTER TABLE children ADD CONSTRAINT PRIMARY KEY(id)",
    ];

    for sql in statements {
        let Statement::AlterTable(statement) = parse_stmt(sql).unwrap() else {
            panic!("expected ALTER TABLE for {sql}");
        };
        assert_eq!(statement.operation, AlterTableOperation::AddConstraint);
        assert!(statement.table_constraint.is_some(), "{sql}");
        assert_eq!(
            statement.to_string().split_whitespace().next(),
            Some("ALTER")
        );
    }

    let Statement::AlterTable(statement) =
        parse_stmt("ALTER TABLE messages ADD COLUMN reply_to UUID REFERENCES messages(id)")
            .unwrap()
    else {
        panic!("expected ALTER TABLE ADD COLUMN");
    };
    assert_eq!(statement.operation, AlterTableOperation::AddColumn);
    assert!(statement.column_def.as_ref().is_some_and(|column| column
        .constraints
        .iter()
        .any(|constraint| matches!(constraint, ColumnConstraint::References { .. }))));
}

#[test]
fn orm_01_parses_drop_constraint_with_if_exists() {
    let Statement::AlterTable(statement) =
        parse_stmt("ALTER TABLE people DROP CONSTRAINT uq_people_email").unwrap()
    else {
        panic!("expected ALTER TABLE DROP CONSTRAINT");
    };
    assert_eq!(statement.operation, AlterTableOperation::DropConstraint);
    assert_eq!(
        statement
            .constraint_name
            .as_ref()
            .map(|name| name.value.as_str()),
        Some("uq_people_email")
    );
    assert!(!statement.if_exists);
    assert_eq!(
        statement.to_string(),
        "ALTER TABLE people DROP CONSTRAINT uq_people_email"
    );

    let Statement::AlterTable(statement) =
        parse_stmt("ALTER TABLE people DROP CONSTRAINT IF EXISTS fk_people_fio___fio").unwrap()
    else {
        panic!("expected ALTER TABLE DROP CONSTRAINT IF EXISTS");
    };
    assert_eq!(statement.operation, AlterTableOperation::DropConstraint);
    assert!(statement.if_exists);
    assert_eq!(
        statement
            .constraint_name
            .as_ref()
            .map(|name| name.value.as_str()),
        Some("fk_people_fio___fio")
    );
}

#[test]
fn orm_02_parses_versioned_json_describe_targets_without_changing_legacy_describe() {
    let Statement::Describe(legacy) = parse_stmt("DESCRIBE people").unwrap() else {
        panic!("expected legacy DESCRIBE");
    };
    assert!(matches!(legacy.target, DescribeTarget::Table(_)));
    assert_eq!(legacy.format, DescribeFormat::Tabular);
    assert_eq!(legacy.to_string(), "DESCRIBE people");

    let Statement::Describe(table) = parse_stmt("DESCRIBE TABLE people FORMAT JSON").unwrap()
    else {
        panic!("expected table JSON DESCRIBE");
    };
    assert!(matches!(table.target, DescribeTarget::Table(_)));
    assert_eq!(table.format, DescribeFormat::Json);
    assert_eq!(table.to_string(), "DESCRIBE TABLE people FORMAT JSON");

    let Statement::Describe(database) = parse_stmt("DESCRIBE DATABASE FORMAT JSON").unwrap() else {
        panic!("expected database JSON DESCRIBE");
    };
    assert_eq!(database.target, DescribeTarget::Database);
    assert_eq!(database.format, DescribeFormat::Json);
    assert_eq!(database.to_string(), "DESCRIBE DATABASE FORMAT JSON");

    assert!(parse_stmt("DESCRIBE DATABASE").is_none());
    assert!(parse_stmt("DESCRIBE TABLE people FORMAT CSV").is_none());
}

#[test]
fn test_parse_drop_table() {
    let stmt = parse_stmt("DROP TABLE IF EXISTS users").unwrap();
    match stmt {
        Statement::DropTable(drop) => {
            assert_eq!(drop.table_name.value, "users");
            assert!(drop.if_exists);
        }
        _ => panic!("expected DropTableStatement"),
    }
}

#[test]
fn test_parse_begin_commit() {
    let stmt = parse_stmt("BEGIN TRANSACTION").unwrap();
    match stmt {
        Statement::Begin(_) => {}
        _ => panic!("expected BeginStatement"),
    }

    let stmt = parse_stmt("COMMIT").unwrap();
    match stmt {
        Statement::Commit(_) => {}
        _ => panic!("expected CommitStatement"),
    }
}

#[test]
fn test_parse_with_cte() {
    let stmt = parse_stmt("WITH temp AS (SELECT * FROM users) SELECT * FROM temp").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.with.is_some());
            let with = select.with.as_ref().unwrap();
            assert_eq!(with.ctes.len(), 1);
            assert_eq!(with.ctes[0].name.value, "temp");
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn test_parse_fetch_first() {
    // FETCH FIRST n ROWS ONLY
    let stmt = parse_stmt("SELECT * FROM users FETCH FIRST 10 ROWS ONLY").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.limit.is_some());
        }
        _ => panic!("expected SelectStatement"),
    }

    // FETCH FIRST n ROW ONLY (singular)
    let stmt = parse_stmt("SELECT * FROM users FETCH FIRST 1 ROW ONLY").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.limit.is_some());
        }
        _ => panic!("expected SelectStatement"),
    }

    // FETCH NEXT n ROWS ONLY
    let stmt = parse_stmt("SELECT * FROM users FETCH NEXT 5 ROWS ONLY").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.limit.is_some());
        }
        _ => panic!("expected SelectStatement"),
    }

    // OFFSET with FETCH
    let stmt = parse_stmt("SELECT * FROM users OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.limit.is_some());
            assert!(select.offset.is_some());
        }
        _ => panic!("expected SelectStatement"),
    }
}

#[test]
fn test_parse_outer_call_with_named_arguments() {
    let statement = parse_stmt("CALL app.recalculate(7, mode => :mode)").unwrap();
    let Statement::Call(call) = statement else {
        panic!("expected CallStatement");
    };
    assert_eq!(call.routine.to_string(), "app.recalculate");
    assert_eq!(call.arguments.len(), 2);
    assert!(call.arguments[0].name.is_none());
    assert_eq!(
        call.arguments[1]
            .name
            .as_ref()
            .map(|name| name.value.as_str()),
        Some("mode")
    );
    assert_eq!(call.to_string(), "CALL app.recalculate(7, mode => :mode)");
}

#[test]
fn parses_security_catalog_surface() {
    let cases = [
        ("CREATE SCHEMA erp", "CREATE SCHEMA erp"),
        ("CREATE PRINCIPAL alice", "CREATE PRINCIPAL alice"),
        ("CREATE ROLE dispatcher", "CREATE ROLE dispatcher"),
        (
            "ALTER PRINCIPAL alice ENABLE",
            "ALTER PRINCIPAL alice ENABLE",
        ),
        (
            "ALTER PRINCIPAL alice DISABLE",
            "ALTER PRINCIPAL alice DISABLE",
        ),
        (
            "ALTER ROLE dispatcher RENAME TO operator",
            "ALTER ROLE dispatcher RENAME TO operator",
        ),
        (
            "DROP PRINCIPAL alice",
            "DROP PRINCIPAL alice RESTRICT",
        ),
        (
            "DROP ROLE dispatcher CASCADE",
            "DROP ROLE dispatcher CASCADE",
        ),
        (
            "GRANT dispatcher TO alice WITH ADMIN OPTION",
            "GRANT dispatcher TO alice WITH ADMIN OPTION",
        ),
        (
            "REVOKE dispatcher FROM alice",
            "REVOKE dispatcher FROM alice RESTRICT",
        ),
        (
            "GRANT SELECT (id, number), UPDATE (status) ON TABLE erp.route_document TO dispatcher",
            "GRANT SELECT (id, number), UPDATE (status) ON TABLE erp.route_document TO dispatcher",
        ),
        (
            "GRANT SELECT (id) ON TABLE erp.route_document TO dispatcher WITH GRANT OPTION",
            "GRANT SELECT (id) ON TABLE erp.route_document TO dispatcher WITH GRANT OPTION",
        ),
        (
            "REVOKE EXECUTE ON PROCEDURE erp.set_status(UUID, BIGINT, TEXT) FROM dispatcher",
            "REVOKE EXECUTE ON PROCEDURE erp.set_status(UUID, BIGINT, TEXT) FROM dispatcher RESTRICT",
        ),
        (
            "REVOKE GRANT OPTION FOR SELECT (id) ON TABLE erp.route_document FROM dispatcher CASCADE",
            "REVOKE GRANT OPTION FOR SELECT (id) ON TABLE erp.route_document FROM dispatcher CASCADE",
        ),
        (
            "REVOKE ADMIN OPTION FOR dispatcher FROM alice",
            "REVOKE ADMIN OPTION FOR dispatcher FROM alice RESTRICT",
        ),
        (
            "GRANT CREATE ON SCHEMA erp TO alice",
            "GRANT CREATE ON SCHEMA erp TO alice",
        ),
        (
            "ALTER TABLE erp.route_document OWNER TO erp_owner",
            "ALTER TABLE erp.route_document OWNER TO erp_owner",
        ),
        (
            "ALTER FUNCTION pricing.line_total(DECIMAL(18, 2), DECIMAL(18, 2)) OWNER TO pricing_owner",
            "ALTER FUNCTION pricing.line_total(DECIMAL(18, 2), DECIMAL(18, 2)) OWNER TO pricing_owner",
        ),
    ];
    for (source, expected) in cases {
        let statements = crate::parse_sql(source).unwrap_or_else(|error| {
            panic!("failed to parse {source:?}: {error}");
        });
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].to_string(), expected);
    }
}

#[test]
fn principal_password_syntax_keeps_ast_value_but_never_formats_the_secret() {
    let secret = "correct horse battery staple";
    let create = crate::parse_sql(&format!("CREATE PRINCIPAL alice PASSWORD '{secret}'"))
        .unwrap()
        .remove(0);
    let Statement::CreatePrincipal(create_principal) = &create else {
        panic!("expected CREATE PRINCIPAL AST");
    };
    assert_eq!(create_principal.password.as_deref(), Some(secret));
    assert_eq!(
        create.to_string(),
        "CREATE PRINCIPAL alice PASSWORD '<redacted>'"
    );
    assert!(!format!("{create:?}").contains(secret));

    let alter = crate::parse_sql(&format!("ALTER PRINCIPAL alice PASSWORD '{secret}'"))
        .unwrap()
        .remove(0);
    let Statement::AlterSecuritySubject(alter_principal) = &alter else {
        panic!("expected ALTER PRINCIPAL AST");
    };
    assert!(matches!(
        &alter_principal.action,
        AlterSecuritySubjectActionSyntax::SetPassword(value) if value == secret
    ));
    assert_eq!(
        alter.to_string(),
        "ALTER PRINCIPAL alice PASSWORD '<redacted>'"
    );
    assert!(!create.to_string().contains(secret));
    assert!(!alter.to_string().contains(secret));
    assert!(!format!("{alter:?}").contains(secret));

    assert_eq!(
        crate::parse_sql("ALTER PRINCIPAL alice PASSWORD NULL").unwrap()[0].to_string(),
        "ALTER PRINCIPAL alice PASSWORD NULL"
    );
    for invalid in [
        "CREATE ROLE reader PASSWORD 'secret'",
        "ALTER ROLE reader PASSWORD 'secret'",
    ] {
        assert!(crate::parse_sql(invalid).is_err(), "must reject {invalid}");
    }
}

#[test]
fn security_parser_rejects_ambiguous_or_invalid_shapes() {
    for source in [
        "CREATE PRINCIPAL app.alice",
        "CREATE ROLE app.dispatcher",
        "GRANT ALL ON TABLE documents TO alice",
        "GRANT DELETE (id) ON TABLE documents TO alice",
        "GRANT SELECT (id, id) ON TABLE documents TO alice",
        "GRANT SELECT, SELECT ON TABLE documents TO alice",
        "GRANT EXECUTE ON PROCEDURE app.run TO alice",
        "ALTER FUNCTION app.run OWNER TO alice",
    ] {
        assert!(
            crate::parse_sql(source).is_err(),
            "invalid security statement unexpectedly parsed: {source}"
        );
    }
}

#[test]
fn ordinary_sql_preserves_dotted_relation_spelling_as_one_storage_identity() {
    let cases = [
        "SELECT * FROM audit.event",
        "INSERT INTO outbox.message (id) VALUES (1)",
        "UPDATE audit.event SET outcome = 'ok'",
        "DELETE FROM audit.event",
        "TRUNCATE TABLE outbox.message",
        "DROP TABLE audit.event",
    ];
    for source in cases {
        let statements = crate::parse_sql(source)
            .unwrap_or_else(|error| panic!("failed to parse {source:?}: {error}"));
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].to_string(), source);
    }
}
