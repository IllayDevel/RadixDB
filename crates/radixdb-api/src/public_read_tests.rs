use super::*;
use radixdb_orm::{
    BinaryOperator, ColumnRef, Expression, IrDocument, JoinKind, Operation, Projection, Relation,
    Select, TypedValue,
};

fn setup() -> (Database, BoundPublicReadPolicy) {
    let database = Database::open_in_memory().unwrap();
    database
        .execute(
            "CREATE TABLE public_items (id INTEGER PRIMARY KEY, name TEXT, secret TEXT)",
            (),
        )
        .unwrap();
    for id in 1..=5 {
        database
            .execute(
                "INSERT INTO public_items (id, name, secret) VALUES ($1, $2, $3)",
                (id, format!("item-{id}"), format!("secret-{id}")),
            )
            .unwrap();
    }
    let policy = database
        .bind_public_read_policy(
            &[PublicReadRelationSpec::new("public_items", ["id", "name"])],
            &[],
        )
        .unwrap();
    (database, policy)
}

fn select(columns: &[(&str, Option<&str>)], filter: Option<Expression>) -> IrDocument {
    let projection = columns
        .iter()
        .map(|(name, alias)| Projection {
            expression: Expression::Column {
                column: ColumnRef::qualified("i", *name),
            },
            alias: alias.map(str::to_owned),
        })
        .collect();
    IrDocument::new(Operation::Select {
        query: Select {
            projection,
            from: Some(Relation::Table {
                name: "public_items".to_owned(),
                alias: Some("i".to_owned()),
            }),
            filter,
            ..Select::default()
        },
    })
}

fn context() -> ServerExecutionContext {
    ServerExecutionContext::for_principal(ObjectId::BOOTSTRAP_OWNER)
}

#[test]
fn keyset_pages_are_complete_ordered_and_non_overlapping() {
    let (database, policy) = setup();
    let request = PublicReadRequest::new(select(&[("id", None), ("name", None)], None), 2);
    let first = database
        .public_read(&request, &context(), &policy, PublicReadLimits::default())
        .unwrap();
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.get(0).unwrap().as_int64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let second = database
        .public_read(
            &request.clone().after(first.next_cursor.unwrap()),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    assert_eq!(
        second
            .rows
            .iter()
            .map(|row| row.get(0).unwrap().as_int64().unwrap())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    let third = database
        .public_read(
            &request.after(second.next_cursor.unwrap()),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    assert_eq!(third.rows.len(), 1);
    assert!(third.next_cursor.is_none());
}

#[test]
fn public_boundary_rejects_dml_unpublished_data_and_functions() {
    let (database, policy) = setup();
    let dml = IrDocument::new(Operation::Delete {
        statement: radixdb_orm::Delete {
            table: "public_items".to_owned(),
            alias: None,
            using: None,
            filter: None,
            all_rows: true,
            returning: Vec::new(),
        },
    });
    let error = database
        .public_read(
            &PublicReadRequest::new(dml, 10),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::UnsupportedShape);

    let secret = PublicReadRequest::new(select(&[("id", None), ("secret", None)], None), 10);
    assert_eq!(
        database
            .public_read(&secret, &context(), &policy, PublicReadLimits::default())
            .unwrap_err()
            .code(),
        PublicReadErrorCode::Policy
    );

    let mut function_query = match select(&[("id", None)], None).payload {
        Operation::Select { query } => query,
        _ => unreachable!(),
    };
    function_query.projection.push(Projection {
        expression: Expression::Function {
            name: "lower".to_owned(),
            arguments: vec![Expression::Column {
                column: ColumnRef::qualified("i", "name"),
            }],
        },
        alias: None,
    });
    let error = database
        .public_read(
            &PublicReadRequest::new(
                IrDocument::new(Operation::Select {
                    query: function_query,
                }),
                10,
            ),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::Policy);
}

#[test]
fn cursor_and_resource_limits_fail_closed_without_value_disclosure() {
    let (database, policy) = setup();
    let document = select(
        &[("id", None), ("name", None)],
        Some(Expression::Binary {
            left: Box::new(Expression::Column {
                column: ColumnRef::qualified("i", "name"),
            }),
            operator: BinaryOperator::Ne,
            right: Box::new(Expression::Literal {
                value: TypedValue::Text("TOP-SECRET-CURSOR-VALUE".to_owned()),
            }),
        }),
    );
    let first = database
        .public_read(
            &PublicReadRequest::new(document.clone(), 2),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    let mut cursor = first.next_cursor.unwrap();
    let replacement = if cursor.fingerprint.starts_with('0') {
        "1"
    } else {
        "0"
    };
    cursor.fingerprint.replace_range(..1, replacement);
    let error = database
        .public_read(
            &PublicReadRequest::new(document.clone(), 2).after(cursor),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::InvalidCursor);
    assert!(!error.to_string().contains("TOP-SECRET"));

    let limits = PublicReadLimits {
        max_scanned_rows: 2,
        ..PublicReadLimits::default()
    };
    let error = database
        .public_read(
            &PublicReadRequest::new(document, 2),
            &context(),
            &policy,
            limits,
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::ResourceLimit);
}

#[test]
fn cursor_is_bound_to_original_parameter_values_but_diagnostics_are_redacted() {
    let (database, policy) = setup();
    let filtered = |value: &str| {
        select(
            &[("id", None), ("name", None)],
            Some(Expression::Binary {
                left: Box::new(Expression::Column {
                    column: ColumnRef::qualified("i", "name"),
                }),
                operator: BinaryOperator::Ne,
                right: Box::new(Expression::Literal {
                    value: TypedValue::Text(value.to_owned()),
                }),
            }),
        )
    };
    let first = database
        .public_read(
            &PublicReadRequest::new(filtered("private-a"), 2),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    let cursor = first.next_cursor.unwrap();
    let error = database
        .public_read(
            &PublicReadRequest::new(filtered("private-b"), 2).after(cursor),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::InvalidCursor);
    assert!(!error.to_string().contains("private-a"));
    assert!(!error.to_string().contains("private-b"));
}

#[test]
fn drop_recreate_does_not_silently_rebind_a_policy() {
    let (database, policy) = setup();
    database.execute("DROP TABLE public_items", ()).unwrap();
    database
        .execute(
            "CREATE TABLE public_items (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .unwrap();
    let error = database
        .public_read(
            &PublicReadRequest::new(select(&[("id", None), ("name", None)], None), 2),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublicReadErrorCode::Policy);
}

#[test]
fn acl_grant_and_revoke_are_rechecked_for_the_same_bound_policy() {
    let (database, policy) = setup();
    database.execute("CREATE PRINCIPAL alice", ()).unwrap();
    database
        .execute("GRANT CONNECT ON DATABASE test TO alice", ())
        .unwrap();
    database
        .execute("GRANT USAGE ON SCHEMA public TO alice", ())
        .unwrap();
    database
        .execute("GRANT SELECT (id) ON TABLE public_items TO alice", ())
        .unwrap();
    let alice = database
        .engine()
        .pin_catalog()
        .unwrap()
        .objects_of_kind(radixdb_catalog::ObjectKind::Principal)
        .find(|object| object.name().normalized().as_str() == "alice")
        .unwrap()
        .id();
    let alice = ServerExecutionContext::for_principal(alice);

    database
        .public_read(
            &PublicReadRequest::new(select(&[("id", None)], None), 2),
            &alice,
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    let id_and_name = PublicReadRequest::new(select(&[("id", None), ("name", None)], None), 2);
    assert_eq!(
        database
            .public_read(&id_and_name, &alice, &policy, PublicReadLimits::default())
            .unwrap_err()
            .code(),
        PublicReadErrorCode::Authorization
    );
    database
        .execute("GRANT SELECT (name) ON TABLE public_items TO alice", ())
        .unwrap();
    database
        .public_read(&id_and_name, &alice, &policy, PublicReadLimits::default())
        .unwrap();
    database
        .execute("REVOKE SELECT (name) ON TABLE public_items FROM alice", ())
        .unwrap();
    assert_eq!(
        database
            .public_read(&id_and_name, &alice, &policy, PublicReadLimits::default())
            .unwrap_err()
            .code(),
        PublicReadErrorCode::Authorization
    );
}

#[test]
fn navigation_requires_every_reference_target_and_column_to_be_published() {
    let database = Database::open_in_memory().unwrap();
    database
        .execute(
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, label TEXT, secret TEXT)",
            (),
        )
        .unwrap();
    database
        .execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, department_id INTEGER REFERENCES departments(id))",
            (),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO departments VALUES (10, 'Finance', 'hidden')",
            (),
        )
        .unwrap();
    database
        .execute("INSERT INTO employees VALUES (1, 10)", ())
        .unwrap();
    let root_only = database
        .bind_public_read_policy(
            &[PublicReadRelationSpec::new(
                "employees",
                ["id", "department_id"],
            )],
            &[],
        )
        .unwrap();
    let document = IrDocument::new(Operation::Select {
        query: Select {
            projection: vec![
                Projection {
                    expression: Expression::Column {
                        column: ColumnRef::qualified("e", "id"),
                    },
                    alias: None,
                },
                Projection {
                    expression: Expression::Navigation {
                        root: "e".to_owned(),
                        path: vec!["department_id".to_owned(), "label".to_owned()],
                    },
                    alias: Some("department_label".to_owned()),
                },
            ],
            from: Some(Relation::Table {
                name: "employees".to_owned(),
                alias: Some("e".to_owned()),
            }),
            ..Select::default()
        },
    });
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document.clone(), 10),
                &context(),
                &root_only,
                PublicReadLimits::default()
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::Policy
    );

    let complete = database
        .bind_public_read_policy(
            &[
                PublicReadRelationSpec::new("employees", ["id", "department_id"]),
                PublicReadRelationSpec::new("departments", ["id", "label"]),
            ],
            &[],
        )
        .unwrap();
    let page = database
        .public_read(
            &PublicReadRequest::new(document, 10),
            &context(),
            &complete,
            PublicReadLimits::default(),
        )
        .unwrap();
    assert_eq!(page.rows[0].get(1).unwrap().as_string().unwrap(), "Finance");
}

#[test]
fn inner_join_uses_all_relation_keys_for_deterministic_pagination() {
    let (database, base_policy) = setup();
    drop(base_policy);
    database
        .execute(
            "CREATE TABLE item_tags (id INTEGER PRIMARY KEY, item_id INTEGER, tag TEXT)",
            (),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO item_tags VALUES (10, 1, 'a'), (11, 1, 'b'), (20, 2, 'c')",
            (),
        )
        .unwrap();
    let policy = database
        .bind_public_read_policy(
            &[
                PublicReadRelationSpec::new("public_items", ["id", "name"]),
                PublicReadRelationSpec::new("item_tags", ["id", "item_id", "tag"]),
            ],
            &[],
        )
        .unwrap();
    let query = Select {
        projection: vec![
            Projection {
                expression: Expression::Column {
                    column: ColumnRef::qualified("i", "id"),
                },
                alias: Some("item_id".to_owned()),
            },
            Projection {
                expression: Expression::Column {
                    column: ColumnRef::qualified("t", "id"),
                },
                alias: Some("tag_id".to_owned()),
            },
            Projection {
                expression: Expression::Column {
                    column: ColumnRef::qualified("t", "tag"),
                },
                alias: None,
            },
        ],
        from: Some(Relation::Join {
            left: Box::new(Relation::Table {
                name: "public_items".to_owned(),
                alias: Some("i".to_owned()),
            }),
            right: Box::new(Relation::Table {
                name: "item_tags".to_owned(),
                alias: Some("t".to_owned()),
            }),
            kind: JoinKind::Inner,
            on: Some(Expression::Binary {
                left: Box::new(Expression::Column {
                    column: ColumnRef::qualified("i", "id"),
                }),
                operator: BinaryOperator::Eq,
                right: Box::new(Expression::Column {
                    column: ColumnRef::qualified("t", "item_id"),
                }),
            }),
        }),
        ..Select::default()
    };
    let request = PublicReadRequest::new(IrDocument::new(Operation::Select { query }), 2);
    let first = database
        .public_read(&request, &context(), &policy, PublicReadLimits::default())
        .unwrap();
    assert_eq!(first.rows.len(), 2);
    let second = database
        .public_read(
            &request.after(first.next_cursor.unwrap()),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert!(second.next_cursor.is_none());
}

#[test]
fn static_and_runtime_byte_limits_are_enforced() {
    let (database, policy) = setup();
    let document = select(&[("id", None), ("name", None)], None);

    let limits = PublicReadLimits {
        max_projection: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document.clone(), 2),
                &context(),
                &policy,
                limits
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let limits = PublicReadLimits {
        max_result_bytes: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document, 2),
                &context(),
                &policy,
                limits
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );
}

#[test]
fn every_public_admission_dimension_has_a_failing_boundary() {
    let (database, policy) = setup();
    let document = select(
        &[("id", None), ("name", None)],
        Some(Expression::Binary {
            left: Box::new(Expression::Column {
                column: ColumnRef::qualified("i", "name"),
            }),
            operator: BinaryOperator::Ne,
            right: Box::new(Expression::Literal {
                value: TypedValue::Text("long-private-parameter".to_owned()),
            }),
        }),
    );

    let limits = PublicReadLimits {
        max_page_size: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document.clone(), 2),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let limits = PublicReadLimits {
        max_filter_nodes: 2,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document.clone(), 1),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let limits = PublicReadLimits {
        max_parameter_bytes: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(document, 1),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let deep_navigation = IrDocument::new(Operation::Select {
        query: Select {
            projection: vec![
                Projection {
                    expression: Expression::Column {
                        column: ColumnRef::qualified("i", "id"),
                    },
                    alias: None,
                },
                Projection {
                    expression: Expression::Navigation {
                        root: "i".to_owned(),
                        path: vec!["a".to_owned(), "b".to_owned()],
                    },
                    alias: None,
                },
            ],
            from: Some(Relation::Table {
                name: "public_items".to_owned(),
                alias: Some("i".to_owned()),
            }),
            ..Select::default()
        },
    });
    let limits = PublicReadLimits {
        max_navigation_depth: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(deep_navigation, 1),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let table = |alias: &str| Relation::Table {
        name: "public_items".to_owned(),
        alias: Some(alias.to_owned()),
    };
    let two_joins = IrDocument::new(Operation::Select {
        query: Select {
            projection: vec![Projection {
                expression: Expression::Column {
                    column: ColumnRef::qualified("i", "id"),
                },
                alias: None,
            }],
            from: Some(Relation::Join {
                left: Box::new(Relation::Join {
                    left: Box::new(table("i")),
                    right: Box::new(table("j")),
                    kind: JoinKind::Cross,
                    on: None,
                }),
                right: Box::new(table("k")),
                kind: JoinKind::Cross,
                on: None,
            }),
            ..Select::default()
        },
    });
    let limits = PublicReadLimits {
        max_joins: 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(two_joins, 1),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );

    let limits = PublicReadLimits {
        max_projection: radixdb_executor::public_read::PUBLIC_READ_MAX_PROJECTION + 1,
        ..PublicReadLimits::default()
    };
    assert_eq!(
        database
            .public_read(
                &PublicReadRequest::new(select(&[("id", None)], None), 1),
                &context(),
                &policy,
                limits,
            )
            .unwrap_err()
            .code(),
        PublicReadErrorCode::ResourceLimit
    );
}

#[test]
fn builtin_function_requires_explicit_policy_allowlist() {
    let (database, _) = setup();
    let policy = database
        .bind_public_read_policy(
            &[PublicReadRelationSpec::new("public_items", ["id", "name"])],
            &["lower".to_owned()],
        )
        .unwrap();
    let mut query = match select(&[("id", None)], None).payload {
        Operation::Select { query } => query,
        _ => unreachable!(),
    };
    query.projection.push(Projection {
        expression: Expression::Function {
            name: "lower".to_owned(),
            arguments: vec![Expression::Column {
                column: ColumnRef::qualified("i", "name"),
            }],
        },
        alias: Some("lower_name".to_owned()),
    });
    let page = database
        .public_read(
            &PublicReadRequest::new(IrDocument::new(Operation::Select { query }), 2),
            &context(),
            &policy,
            PublicReadLimits::default(),
        )
        .unwrap();
    assert_eq!(page.rows.len(), 2);
}

#[test]
fn catalog_fence_covers_binding_authorization_and_complete_consumption() {
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    let (database, policy) = setup();
    let database_id = database
        .engine()
        .pin_catalog()
        .unwrap()
        .meta()
        .database_id();
    let relation_id = policy.relation("public_items").unwrap().object_id;
    let peer = radixdb_executor::Executor::new(Arc::clone(database.engine()));
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let _hook = radixdb_executor::public_read::PublicReadFenceTestHookGuard::install(
        database_id,
        relation_id,
        Arc::new(move || {
            reached_tx.send(()).unwrap();
            release_rx.lock().unwrap().take().unwrap().recv().unwrap();
        }),
    );
    let reader = {
        let database = database.clone();
        thread::spawn(move || {
            database.public_read(
                &PublicReadRequest::new(select(&[("id", None), ("name", None)], None), 5),
                &context(),
                &policy,
                PublicReadLimits::default(),
            )
        })
    };
    reached_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (ddl_tx, ddl_rx) = mpsc::channel();
    let ddl = thread::spawn(move || {
        let result = peer.execute("DROP TABLE public_items");
        ddl_tx.send(result).unwrap();
    });
    assert!(ddl_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).unwrap();
    assert_eq!(reader.join().unwrap().unwrap().rows.len(), 5);
    ddl_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    ddl.join().unwrap();
}
