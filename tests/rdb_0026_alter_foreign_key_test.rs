//! Messenger RDB-0026: ALTER must publish CREATE-equivalent foreign keys.

use radixdb::{Database, Result};
use tempfile::TempDir;

const PARENT: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e01";
const CHILD: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e02";
const ORPHAN: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7eff";
const MESSAGE: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e03";

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

fn show_create(db: &Database, table: &str) -> Result<String> {
    let rows: Vec<_> = db
        .query(&format!("SHOW CREATE TABLE {table}"), ())?
        .collect();
    Ok(rows[0].as_ref().unwrap().get(1).unwrap())
}

#[test]
fn r4_l05_messenger_contracts_alter_foreign_key_is_validated_transactional_and_durable(
) -> Result<()> {
    let dir = TempDir::new()?;
    {
        let db = open(dir.path())?;
        db.execute("CREATE TABLE parents (id UUID PRIMARY KEY)", ())?;
        db.execute(
            "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID)",
            (),
        )?;
        db.execute(&format!("INSERT INTO parents VALUES ('{PARENT}')"), ())?;
        db.execute(
            &format!("INSERT INTO children VALUES ('{CHILD}', '{ORPHAN}')"),
            (),
        )?;

        // Existing orphaned data rejects the migration and publishes nothing.
        db.execute("BEGIN", ())?;
        assert!(db
            .execute(
                "ALTER TABLE children ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parents(id)",
                (),
            )
            .is_err());
        db.execute("ROLLBACK", ())?;
        assert!(!show_create(&db, "children")?.contains("FOREIGN KEY"));

        db.execute(
            &format!("UPDATE children SET parent_id = '{PARENT}' WHERE id = '{CHILD}'"),
            (),
        )?;
        db.execute("BEGIN", ())?;
        db.execute(
            "ALTER TABLE children ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE RESTRICT",
            (),
        )?;
        db.execute("COMMIT", ())?;
        assert!(show_create(&db, "children")?.contains("FOREIGN KEY"));
        assert!(db
            .execute(
                &format!("INSERT INTO children VALUES ('{ORPHAN}', '{ORPHAN}')"),
                (),
            )
            .is_err());
        assert!(db
            .execute(&format!("DELETE FROM parents WHERE id = '{PARENT}'"), ())
            .is_err());

        // The exact clean-migration form used by Messenger is also supported.
        db.execute("CREATE TABLE messages (id UUID PRIMARY KEY)", ())?;
        db.execute(&format!("INSERT INTO messages VALUES ('{MESSAGE}')"), ())?;
        db.execute("BEGIN", ())?;
        db.execute(
            "ALTER TABLE messages ADD COLUMN reply_to_message_id UUID REFERENCES messages(id)",
            (),
        )?;
        db.execute("COMMIT", ())?;
        let messages_ddl = show_create(&db, "messages")?;
        assert!(
            messages_ddl.contains("FOREIGN KEY") && messages_ddl.contains("reply_to_message_id"),
            "{messages_ddl}"
        );
        assert!(db
            .execute(
                &format!(
                    "INSERT INTO messages (id, reply_to_message_id) VALUES ('{ORPHAN}', '{ORPHAN}')"
                ),
                (),
            )
            .is_err());

        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(dir.path())?;
        assert!(show_create(&db, "children")?.contains("FOREIGN KEY"));
        assert!(show_create(&db, "messages")?.contains("FOREIGN KEY"));
        assert!(db
            .execute(
                &format!("INSERT INTO children VALUES ('{ORPHAN}', '{ORPHAN}')"),
                (),
            )
            .is_err());
    }
    Ok(())
}
