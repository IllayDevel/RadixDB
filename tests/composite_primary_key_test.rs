use radixdb::{Database, Result};
use tempfile::TempDir;

const FIRST_ID: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e01";
const SECOND_ID: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e02";

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

#[test]
fn composite_and_text_primary_keys_are_enforced_before_and_after_reopen() -> Result<()> {
    let directory = TempDir::new()?;

    {
        let db = open(directory.path())?;
        db.execute(
            "CREATE TABLE messages (
                id UUID NOT NULL,
                happened TIMESTAMP NOT NULL,
                payload TEXT,
                PRIMARY KEY (happened, id)
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE memberships (
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                PRIMARY KEY (user_id, group_id)
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE text_identity (id TEXT PRIMARY KEY, payload TEXT)",
            (),
        )?;

        db.execute(
            &format!("INSERT INTO messages VALUES ('{FIRST_ID}', '2026-09-11 10:00:00', 'first')"),
            (),
        )?;
        db.execute(
            &format!(
                "INSERT INTO messages VALUES ('{FIRST_ID}', '2026-09-11 10:00:01', 'same id')"
            ),
            (),
        )?;
        assert!(db
            .execute(
                &format!(
                    "INSERT INTO messages VALUES ('{FIRST_ID}', '2026-09-11 10:00:00', 'duplicate')"
                ),
                (),
            )
            .is_err());

        db.execute("INSERT INTO memberships VALUES ('admins', 'alice')", ())?;
        db.execute("INSERT INTO memberships VALUES ('users', 'alice')", ())?;
        assert!(db
            .execute("INSERT INTO memberships VALUES ('admins', 'alice')", ())
            .is_err());

        db.execute("INSERT INTO text_identity VALUES ('alpha', 'first')", ())?;
        assert!(db
            .execute(
                "INSERT INTO text_identity VALUES ('alpha', 'duplicate')",
                ()
            )
            .is_err());
        assert!(db
            .execute("INSERT INTO memberships VALUES (NULL, 'nobody')", ())
            .is_err());

        assert_eq!(
            db.query_one::<i64, _>(
                &format!(
                    "SELECT COUNT(*) FROM messages WHERE happened = '2026-09-11 10:00:00' AND id = '{FIRST_ID}'"
                ),
                (),
            )?,
            1
        );

        db.execute("CREATE TABLE promoted (left_key TEXT, right_key TEXT)", ())?;
        db.execute(
            "ALTER TABLE promoted ADD CONSTRAINT PRIMARY KEY (right_key, left_key)",
            (),
        )?;
        db.execute("INSERT INTO promoted VALUES ('left', 'right')", ())?;
        assert!(db
            .execute("INSERT INTO promoted VALUES ('left', 'right')", ())
            .is_err());
        db.execute("ALTER TABLE promoted DROP CONSTRAINT pk_promoted", ())?;
        db.execute("INSERT INTO promoted VALUES ('left', 'right')", ())?;

        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(directory.path())?;
        assert!(db
            .execute(
                &format!(
                    "INSERT INTO messages VALUES ('{FIRST_ID}', '2026-09-11 10:00:00', 'cold duplicate')"
                ),
                (),
            )
            .is_err());
        db.execute(
            &format!(
                "INSERT INTO messages VALUES ('{SECOND_ID}', '2026-09-11 10:00:00', 'new key')"
            ),
            (),
        )?;
        assert!(db
            .execute("INSERT INTO memberships VALUES ('admins', 'alice')", ())
            .is_err());
        assert!(db
            .execute(
                "INSERT INTO text_identity VALUES ('alpha', 'cold duplicate')",
                ()
            )
            .is_err());
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM promoted", ())?,
            2
        );
        db.close()?;
    }

    Ok(())
}
