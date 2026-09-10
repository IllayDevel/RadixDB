//! R4-L03 batch A: column constraints must have one fail-closed lifecycle.

use radixdb::{Database, Result};
use tempfile::TempDir;

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

fn show_create(db: &Database) -> Result<String> {
    let rows: Vec<_> = db.query("SHOW CREATE TABLE devices", ())?.collect();
    Ok(rows[0].as_ref().unwrap().get(1).unwrap())
}

#[test]
fn r4_l05_messenger_contracts_column_check_lifecycle_is_atomic_and_durable() -> Result<()> {
    let dir = TempDir::new()?;

    {
        let db = open(dir.path())?;
        db.execute(
            "CREATE TABLE devices (id INTEGER PRIMARY KEY, platform TEXT NOT NULL CHECK (platform IN ('linux', 'android')))",
            (),
        )?;
        db.execute(
            "INSERT INTO devices VALUES (1, 'linux'), (2, 'android')",
            (),
        )?;

        // The replacement validates existing rows and atomically publishes the
        // new effective constraint.
        db.execute(
            "ALTER TABLE devices MODIFY COLUMN platform TEXT NOT NULL CHECK (platform IN ('linux', 'android', 'bot_api'))",
            (),
        )?;
        db.execute("INSERT INTO devices VALUES (3, 'bot_api')", ())?;
        assert!(db
            .execute("INSERT INTO devices VALUES (4, 'invalid')", ())
            .is_err());

        db.execute("BEGIN", ())?;
        db.execute(
            "ALTER TABLE devices MODIFY COLUMN platform TEXT NOT NULL CHECK (platform IN ('linux', 'android', 'bot_api', 'desktop'))",
            (),
        )?;
        db.execute("INSERT INTO devices VALUES (4, 'desktop')", ())?;
        db.execute("COMMIT", ())?;
        assert!(show_create(&db)?.contains("desktop"));

        // A replacement incompatible with existing data must leave the prior
        // schema and data untouched.
        assert!(db
            .execute(
                "ALTER TABLE devices MODIFY COLUMN platform TEXT NOT NULL CHECK (platform IN ('linux'))",
                (),
            )
            .is_err());
        db.execute("INSERT INTO devices VALUES (5, 'bot_api')", ())?;

        // Until expression rewriting is a first-class operation, rename is
        // rejected before mutation rather than orphaning the CHECK text.
        assert!(db
            .execute(
                "ALTER TABLE devices RENAME COLUMN platform TO client_platform",
                (),
            )
            .is_err());
        let rendered = show_create(&db)?;
        assert!(rendered.contains("platform"));
        assert!(rendered.contains("bot_api"));

        // ADD COLUMN now publishes the same CHECK contract as CREATE TABLE.
        db.execute(
            "ALTER TABLE devices ADD COLUMN state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'offline'))",
            (),
        )?;
        assert!(db
            .execute(
                "INSERT INTO devices (id, platform, state) VALUES (6, 'linux', 'broken')",
                (),
            )
            .is_err());
        for sql in [
            "ALTER TABLE devices ADD COLUMN bad_pk INTEGER PRIMARY KEY",
            "ALTER TABLE devices ADD COLUMN bad_auto INTEGER AUTO_INCREMENT",
            "ALTER TABLE devices ADD COLUMN bad_nn TEXT NOT NULL",
        ] {
            assert!(db.execute(sql, ()).is_err(), "must fail closed: {sql}");
        }

        // Multiple or malformed column CHECK clauses are rejected rather than
        // retaining only the first or publishing an unevaluable expression.
        for sql in [
            "ALTER TABLE devices ADD COLUMN bad_multi INTEGER CHECK (bad_multi > 0) CHECK (bad_multi < 10)",
            "ALTER TABLE devices ADD COLUMN bad_expr INTEGER CHECK (missing_column > 0)",
            "CREATE TABLE bad_create (id INTEGER CHECK (id > 0) CHECK (id < 10))",
        ] {
            assert!(db.execute(sql, ()).is_err(), "must fail closed: {sql}");
        }

        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(dir.path())?;
        db.execute(
            "INSERT INTO devices (id, platform, state) VALUES (7, 'bot_api', 'ready')",
            (),
        )?;
        assert!(db
            .execute(
                "INSERT INTO devices (id, platform, state) VALUES (8, 'invalid', 'ready')",
                (),
            )
            .is_err());
        let rendered = show_create(&db)?;
        assert!(rendered.contains("platform"));
        assert!(rendered.contains("bot_api"));
    }

    Ok(())
}
