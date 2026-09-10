//! ORM-01: automatic names and durable DROP CONSTRAINT semantics.

use radixdb::{Database, Result};
use radixdb_orm::{DescriptorEnvelope, DescriptorKind, TableDescriptor};
use tempfile::TempDir;

const PARENT: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e01";
const CHILD: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e02";
const CHILD_2: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e03";
const ORPHAN: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7eff";

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

fn table_catalog_id(db: &Database, table: &str) -> Result<String> {
    let json = db.query_one::<String, _>(&format!("DESCRIBE TABLE {table} FORMAT JSON"), ())?;
    let descriptor = DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)
        .map_err(|error| radixdb::Error::internal(error.to_string()))?;
    Ok(descriptor.payload.catalog_id)
}

#[test]
fn orm_01_drop_constraint_is_atomic_enforced_and_durable() -> Result<()> {
    let directory = TempDir::new()?;
    let children_catalog_id;
    {
        let db = open(directory.path())?;
        db.execute("CREATE TABLE parents (id UUID PRIMARY KEY)", ())?;
        db.execute(
            "CREATE TABLE children (
                id UUID PRIMARY KEY,
                parent_id UUID REFERENCES parents(id),
                code TEXT UNIQUE,
                score INTEGER,
                CHECK (score >= 0)
            )",
            (),
        )?;
        db.execute(&format!("INSERT INTO parents VALUES ('{PARENT}')"), ())?;
        db.execute(
            &format!("INSERT INTO children VALUES ('{CHILD}', '{PARENT}', 'one', 1)"),
            (),
        )?;

        // A referenced target key cannot disappear before its FK edge.
        assert!(db
            .execute("ALTER TABLE parents DROP CONSTRAINT pk_parents", ())
            .is_err());

        // Transaction rollback retains both catalog identity and enforcement.
        db.execute("BEGIN", ())?;
        db.execute("ALTER TABLE children DROP CONSTRAINT uq_children_code", ())?;
        db.execute("ROLLBACK", ())?;
        assert!(db
            .execute(
                &format!("INSERT INTO children VALUES ('{CHILD_2}', '{PARENT}', 'one', 2)"),
                (),
            )
            .is_err());

        db.execute("BEGIN", ())?;
        db.execute("ALTER TABLE children DROP CONSTRAINT uq_children_code", ())?;
        db.execute(
            &format!("INSERT INTO children VALUES ('{CHILD_2}', '{PARENT}', 'one', 2)"),
            (),
        )?;
        db.execute("COMMIT", ())?;

        db.execute("ALTER TABLE children DROP CONSTRAINT chk_children_1", ())?;
        db.execute(
            "ALTER TABLE children DROP CONSTRAINT fk_children_parent_id___parents",
            (),
        )?;
        db.execute("BEGIN", ())?;
        db.execute("ALTER TABLE children DROP CONSTRAINT pk_children", ())?;
        db.execute(
            &format!("INSERT INTO children VALUES ('{CHILD}', '{ORPHAN}', 'one', -1)"),
            (),
        )?;
        db.execute("COMMIT", ())?;
        db.execute(
            "ALTER TABLE children DROP CONSTRAINT IF EXISTS uq_children_missing",
            (),
        )?;
        db.execute("ALTER TABLE parents DROP CONSTRAINT pk_parents", ())?;

        children_catalog_id = table_catalog_id(&db, "children")?;
        assert_ne!(children_catalog_id, uuid::Uuid::nil().to_string());

        // All four removed owners are immediately absent.
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(directory.path())?;
        assert_eq!(table_catalog_id(&db, "children")?, children_catalog_id);
        db.execute(
            &format!("INSERT INTO children VALUES ('{CHILD}', '{ORPHAN}', 'one', -2)"),
            (),
        )?;
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM children", ())?,
            4
        );
        db.close()?;
    }
    Ok(())
}

fn index_names(db: &Database, table: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for row in db.query(&format!("SHOW INDEXES FROM {table}"), ())? {
        names.push(row?.get::<String>(1)?);
    }
    names.sort();
    Ok(names)
}

#[test]
fn orm_01_generated_names_survive_rename_and_collision() -> Result<()> {
    let db = Database::open("memory://orm-01-renames")?;
    assert!(db
        .execute(
            "CREATE TABLE rejected (id INTEGER, CONSTRAINT custom UNIQUE (id))",
            (),
        )
        .is_err());
    db.execute("CREATE TABLE rejected_alter (id INTEGER)", ())?;
    assert!(db
        .execute(
            "ALTER TABLE rejected_alter ADD CONSTRAINT custom UNIQUE (id)",
            (),
        )
        .is_err());

    db.execute(
        "CREATE TABLE collision (
            id INTEGER PRIMARY KEY,
            a_b TEXT,
            c TEXT,
            a TEXT,
            b_c TEXT,
            UNIQUE (a_b, c),
            UNIQUE (a, b_c)
        )",
        (),
    )?;
    let before = index_names(&db, "collision")?;
    assert!(before.iter().any(|name| name == "uq_collision_a_b_c"));
    let hashed = before
        .iter()
        .find(|name| name.starts_with("uq_collision_a_b_c__"))
        .cloned()
        .expect("colliding generated name must receive a deterministic suffix");
    assert_eq!(hashed.rsplit_once("__").unwrap().1.len(), 8);

    db.execute("ALTER TABLE collision RENAME COLUMN a_b TO renamed", ())?;
    db.execute("ALTER TABLE collision RENAME TO moved", ())?;
    assert_eq!(index_names(&db, "moved")?, before);
    db.execute("ALTER TABLE moved DROP CONSTRAINT uq_collision_a_b_c", ())?;
    db.execute(
        &format!("ALTER TABLE moved DROP CONSTRAINT \"{hashed}\""),
        (),
    )?;
    let remaining = index_names(&db, "moved")?;
    assert!(!remaining
        .iter()
        .any(|name| name.starts_with("uq_collision")));
    Ok(())
}

#[test]
fn orm_01_transactional_constraint_savepoint_and_net_zero_pk() -> Result<()> {
    let db = Database::open("memory://orm-01-savepoint")?;
    db.execute(
        "CREATE TABLE guarded_savepoint (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
        (),
    )?;
    db.execute("INSERT INTO guarded_savepoint VALUES (1, 'same')", ())?;

    db.execute("BEGIN", ())?;
    db.execute("SAVEPOINT before_drop", ())?;
    db.execute(
        "ALTER TABLE guarded_savepoint DROP CONSTRAINT uq_guarded_savepoint_code",
        (),
    )?;
    db.execute("INSERT INTO guarded_savepoint VALUES (2, 'same')", ())?;
    db.execute("ROLLBACK TO SAVEPOINT before_drop", ())?;
    assert!(db
        .execute("INSERT INTO guarded_savepoint VALUES (2, 'same')", ())
        .is_err());
    db.execute("COMMIT", ())?;

    db.execute("CREATE TABLE pk_cycle (id INTEGER, payload TEXT)", ())?;
    db.execute("BEGIN", ())?;
    db.execute("ALTER TABLE pk_cycle ADD CONSTRAINT PRIMARY KEY (id)", ())?;
    db.execute("ALTER TABLE pk_cycle DROP CONSTRAINT pk_pk_cycle", ())?;
    db.execute("INSERT INTO pk_cycle VALUES (7, 'first')", ())?;
    db.execute("INSERT INTO pk_cycle VALUES (7, 'second')", ())?;
    db.execute("COMMIT", ())?;
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM pk_cycle", ())?,
        2
    );
    Ok(())
}
