//! Messenger RDB-0027: a LIMIT applies to joined results, never outer candidates.

use radixdb::{Database, Result};
use tempfile::TempDir;

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

fn authorized_attachment(db: &Database, attachment: &str, user: &str) -> Result<Option<String>> {
    let sql = format!(
        "SELECT a.id
         FROM forwarded_message_attachments fma
         INNER JOIN messages m ON m.id = fma.message_id
         INNER JOIN conversation_members cm ON cm.conversation_id = m.conversation_id
         INNER JOIN attachments a ON a.id = fma.attachment_id
         WHERE fma.attachment_id = '{attachment}'
           AND cm.user_id = '{user}'
           AND cm.left_at IS NULL
           AND m.conversation_seq > cm.joined_seq
           AND m.deleted_at IS NULL
           AND a.purpose = 'message'
           AND a.state = 'ready'
         LIMIT 1"
    );
    let mut rows = db.query(&sql, ())?;
    match rows.next() {
        Some(row) => Ok(Some(row?.get(0)?)),
        None => Ok(None),
    }
}

#[test]
fn r4_l05_messenger_contracts_forwarded_attachment_authorization_is_order_independent() -> Result<()>
{
    let dir = TempDir::new()?;
    let user = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7101";
    let good_conversation = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7102";
    let bad_conversation = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7103";
    let good_message = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7104";
    let bad_message = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7105";
    let attachment_a = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7106";
    let attachment_b = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7107";

    {
        let db = open(dir.path())?;
        db.execute(
            "CREATE TABLE messages (id UUID PRIMARY KEY, conversation_id UUID NOT NULL, conversation_seq INTEGER NOT NULL, deleted_at TIMESTAMP)",
            (),
        )?;
        db.execute(
            "CREATE TABLE conversation_members (id UUID PRIMARY KEY, conversation_id UUID NOT NULL, user_id UUID NOT NULL, joined_seq INTEGER NOT NULL, left_at TIMESTAMP)",
            (),
        )?;
        db.execute(
            "CREATE TABLE attachments (id UUID PRIMARY KEY, purpose TEXT NOT NULL, state TEXT NOT NULL)",
            (),
        )?;
        db.execute(
            "CREATE TABLE forwarded_message_attachments (id UUID PRIMARY KEY, message_id UUID NOT NULL, attachment_id UUID NOT NULL)",
            (),
        )?;
        db.execute(
            "CREATE INDEX fma_attachment_idx ON forwarded_message_attachments (attachment_id)",
            (),
        )?;
        db.execute(
            "CREATE INDEX member_conversation_idx ON conversation_members (conversation_id)",
            (),
        )?;

        db.execute(
            &format!(
                "INSERT INTO messages VALUES
                 ('{bad_message}', '{bad_conversation}', 1, NULL),
                 ('{good_message}', '{good_conversation}', 2, NULL)"
            ),
            (),
        )?;
        db.execute(
            &format!(
                "INSERT INTO conversation_members VALUES
                 ('018f2b34-7a10-7cc2-8f3a-9d4b5c6d7110', '{good_conversation}', '{user}', 0, NULL)"
            ),
            (),
        )?;
        db.execute(
            &format!(
                "INSERT INTO attachments VALUES
                 ('{attachment_a}', 'message', 'ready'),
                 ('{attachment_b}', 'message', 'ready')"
            ),
            (),
        )?;

        // A: ineligible relation first. B: eligible relation first.
        db.execute(
            &format!(
                "INSERT INTO forwarded_message_attachments VALUES
                 ('018f2b34-7a10-7cc2-8f3a-9d4b5c6d7120', '{bad_message}', '{attachment_a}'),
                 ('018f2b34-7a10-7cc2-8f3a-9d4b5c6d7121', '{good_message}', '{attachment_a}'),
                 ('018f2b34-7a10-7cc2-8f3a-9d4b5c6d7122', '{good_message}', '{attachment_b}'),
                 ('018f2b34-7a10-7cc2-8f3a-9d4b5c6d7123', '{bad_message}', '{attachment_b}')"
            ),
            (),
        )?;
        assert_eq!(
            authorized_attachment(&db, attachment_a, user)?,
            Some(attachment_a.into())
        );
        assert_eq!(
            authorized_attachment(&db, attachment_b, user)?,
            Some(attachment_b.into())
        );
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(dir.path())?;
        assert_eq!(
            authorized_attachment(&db, attachment_a, user)?,
            Some(attachment_a.into())
        );
        assert_eq!(
            authorized_attachment(&db, attachment_b, user)?,
            Some(attachment_b.into())
        );
    }
    Ok(())
}
