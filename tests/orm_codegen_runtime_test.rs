use std::fs;
use std::process::Command;

use radixdb::Database;
use radixdb_orm::{generate_rust_database, DatabaseDescriptor, DescriptorEnvelope, DescriptorKind};

/// This gate compiles the deterministic generated facade as an independent
/// consumer and then runs it against the exact persisted catalog it came from.
/// It is intentionally explicit because spawning a nested clean Cargo build is
/// too expensive for every ordinary unit-test invocation.
#[test]
#[ignore = "explicit generated-facade release gate"]
fn generated_facade_executes_crud_and_rejects_stale_schema() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("database");
    let dsn = format!("file://{}", database_path.display());

    let descriptor = {
        let db = Database::open(&dsn).expect("open generated-facade fixture");
        db.execute(
            "CREATE TABLE people (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new'
            )",
            (),
        )
        .expect("create generated-facade fixture");
        let json = db
            .query_one::<String, _>("DESCRIBE DATABASE FORMAT JSON", ())
            .expect("database descriptor");
        db.close().expect("close descriptor fixture");
        DescriptorEnvelope::<DatabaseDescriptor>::from_json(&json, DescriptorKind::Database)
            .expect("decode descriptor")
    };

    let mut generated = generate_rust_database(&descriptor)
        .expect("generate facade from saved descriptor")
        .source;
    generated.push_str(&format!(
        r#"
#[cfg(test)]
mod runtime_gate {{
    use super::*;
    use radixdb::Database;
    use radixdb_orm::FieldValue;

    #[test]
    fn generated_crud_and_stale_schema() {{
        let db = Database::open({dsn:?}).expect("open persisted fixture");
        let mut person = People::new();
        person.id.set(7);
        person.name.set("Alice".to_string());
        person.insert(&db).expect("generated INSERT");
        assert!(matches!(person.status.value(), FieldValue::Value {{ value }} if value == "new"));
        assert!(!person.name.is_dirty());

        let mut fetched = People::get(7).one(&db).expect("generated SELECT");
        assert!(matches!(fetched.name.value(), FieldValue::Value {{ value }} if value == "Alice"));
        fetched.name.set("Updated".to_string());
        fetched.save(&db).expect("generated PK SAVE");
        let updated = People::get(7).one(&db).expect("generated SELECT after SAVE");
        assert!(matches!(updated.name.value(), FieldValue::Value {{ value }} if value == "Updated"));

        db.execute("ALTER TABLE people ADD COLUMN note TEXT", ())
            .expect("alter schema");
        let error = People::get(7).one(&db).expect_err("stale facade must fail closed");
        assert!(error.to_string().contains("generated schema changed"), "{{error}}");
    }}
}}
"#,
        dsn = dsn,
    ));

    let crate_dir = temp.path().join("consumer");
    fs::create_dir_all(crate_dir.join("src")).expect("create consumer crate");
    let root = env!("CARGO_MANIFEST_DIR");
    fs::write(
        crate_dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"radixdb-orm-runtime-probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[dependencies]\nradixdb = {{ path = {root:?} }}\nradixdb-orm = {{ path = {orm:?} }}\nserde_json = \"1\"\n",
            root = root,
            orm = format!("{root}/crates/radixdb-orm"),
        ),
    )
    .expect("write consumer manifest");
    fs::write(crate_dir.join("src/lib.rs"), generated).expect("write generated consumer");

    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["test", "--offline", "--quiet", "--", "--test-threads=1"])
        .current_dir(&crate_dir)
        .env("CARGO_TARGET_DIR", temp.path().join("target"))
        // The parent test binary records dirty state in its public build
        // identity, while the nested source build accepts only the underlying
        // 40-hex revision as an explicit override.
        .env(
            "RADIXDB_GIT_COMMIT",
            env!("RADIXDB_GIT_COMMIT").trim_end_matches("-dirty"),
        )
        .status()
        .expect("run generated consumer");
    assert!(status.success(), "generated facade runtime consumer failed");
}
