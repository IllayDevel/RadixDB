//! ALTER TABLE must publish every constraint family accepted by CREATE TABLE.

use radixdb::{Database, Result};

#[test]
fn r4_l05_messenger_contracts_alter_table_constraint_families_are_atomic_and_enforced() -> Result<()>
{
    let db = Database::open("memory://alter_create_constraint_parity")?;

    db.execute(
        "CREATE TABLE accounts (id INTEGER, email TEXT, score INTEGER)",
        (),
    )?;
    db.execute(
        "INSERT INTO accounts VALUES (1, 'a@example.test', 10), (2, 'b@example.test', 20)",
        (),
    )?;
    db.execute("BEGIN", ())?;
    db.execute("ALTER TABLE accounts ADD CONSTRAINT UNIQUE(email)", ())?;
    db.execute("ALTER TABLE accounts ADD CONSTRAINT CHECK(score >= 0)", ())?;
    db.execute("COMMIT", ())?;
    assert!(db
        .execute("INSERT INTO accounts VALUES (3, 'a@example.test', 30)", (),)
        .is_err());
    assert!(db
        .execute("INSERT INTO accounts VALUES (3, 'c@example.test', -1)", ())
        .is_err());

    // Invalid existing data rejects the statement and retains the old schema.
    assert!(db
        .execute("ALTER TABLE accounts ADD CONSTRAINT CHECK(score > 100)", ())
        .is_err());
    db.execute("INSERT INTO accounts VALUES (3, 'c@example.test', 30)", ())?;

    db.execute("CREATE TABLE identities (id INTEGER, label TEXT)", ())?;
    db.execute("ALTER TABLE identities ADD CONSTRAINT PRIMARY KEY(id)", ())?;
    db.execute("INSERT INTO identities VALUES (1, 'one')", ())?;
    assert!(db
        .execute("INSERT INTO identities VALUES (1, 'duplicate')", ())
        .is_err());

    db.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY)", ())?;
    db.execute("ALTER TABLE tags ADD COLUMN name TEXT UNIQUE", ())?;
    db.execute("INSERT INTO tags VALUES (1, 'one')", ())?;
    assert!(db
        .execute("INSERT INTO tags VALUES (2, 'one')", ())
        .is_err());

    db.execute("CREATE TABLE generated (label TEXT)", ())?;
    db.execute(
        "ALTER TABLE generated ADD COLUMN id INTEGER PRIMARY KEY AUTO_INCREMENT",
        (),
    )?;
    db.execute("INSERT INTO generated (label) VALUES ('one'), ('two')", ())?;
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(DISTINCT id) FROM generated", ())?,
        2
    );

    // Messenger migration 0033 adds REFERENCES and then strengthens the same
    // column with a named UNIQUE index inside one migration transaction.
    db.execute("CREATE TABLE users (id UUID PRIMARY KEY)", ())?;
    db.execute(
        "CREATE TABLE conversations (id UUID PRIMARY KEY, title TEXT)",
        (),
    )?;
    db.execute("BEGIN", ())?;
    db.execute(
        "ALTER TABLE conversations ADD COLUMN self_owner_id UUID REFERENCES users(id)",
        (),
    )?;
    db.execute(
        "CREATE UNIQUE INDEX conversations_self_owner_uidx ON conversations (self_owner_id)",
        (),
    )?;
    db.execute("COMMIT", ())?;

    Ok(())
}
