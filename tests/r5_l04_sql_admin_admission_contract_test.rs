use radixdb::Database;

#[test]
fn r5_l04_batch_i_unknown_set_is_rejected() {
    let db = Database::open("memory://r5_l04_unknown_set").unwrap();
    assert!(
        db.execute("SET imaginary_setting = 1", ()).is_err(),
        "unknown SET variables must not report success"
    );
}

#[test]
fn r5_l04_batch_i_non_hnsw_index_options_are_rejected() {
    let db = Database::open("memory://r5_l04_index_options").unwrap();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .unwrap();
    assert!(
        db.execute(
            "CREATE INDEX items_value_idx ON items(value) USING BTREE WITH (m = 16)",
            (),
        )
        .is_err(),
        "options without a non-HNSW consumer must not be ignored"
    );
    assert!(
        db.execute("DROP INDEX items_value_idx", ()).is_err(),
        "rejected CREATE INDEX must not publish an index"
    );
}

#[test]
fn r5_l04_batch_i_analyze_table_requires_a_name() {
    let db = Database::open("memory://r5_l04_analyze_table_name").unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    assert!(
        db.execute("ANALYZE TABLE", ()).is_err(),
        "malformed ANALYZE TABLE must not expand to ANALYZE ALL"
    );
}

#[test]
fn r5_l04_batch_i_unsupported_isolation_aliases_are_rejected() {
    for (suffix, level) in [
        ("serializable", "SERIALIZABLE"),
        ("repeatable", "REPEATABLE READ"),
        ("uncommitted", "READ UNCOMMITTED"),
    ] {
        let db = Database::open(&format!("memory://r5_l04_isolation_{suffix}")).unwrap();
        assert!(
            db.execute(&format!("BEGIN TRANSACTION ISOLATION LEVEL {level}"), (),)
                .is_err(),
            "unsupported isolation level {level} must not be silently downgraded"
        );
        assert!(
            db.execute(&format!("SET ISOLATION_LEVEL = '{level}'"), (),)
                .is_err(),
            "SET must reject unsupported isolation level {level}"
        );
    }
}

#[test]
fn r5_l04_batch_i_pragma_numeric_admission_is_exact() {
    let db = Database::open("memory://r5_l04_pragma_numeric").unwrap();

    assert!(
        db.execute("PRAGMA checkpoint_interval = 1.5", ()).is_err(),
        "fractional PRAGMA values must not be truncated"
    );
    assert!(
        db.execute("PRAGMA checkpoint_interval = 4294967296", ())
            .is_err(),
        "u32 PRAGMA values must not wrap"
    );

    db.execute("PRAGMA checkpoint_interval = 17", ()).unwrap();
    let installed: i64 = db.query_one("PRAGMA checkpoint_interval", ()).unwrap();
    assert_eq!(installed, 17, "reported value must equal installed config");
}
