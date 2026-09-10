use radixdb_client::{Connection, ExecuteResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15441".to_string());

    let mut db = Connection::connect(address)?;
    db.authenticate("root", None)?;
    db.select_database("test")?;

    db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.execute("DELETE FROM users WHERE id = 1")?;
    db.execute("INSERT INTO users VALUES (1, 'Alice')")?;

    match db.execute("SELECT id, name FROM users WHERE id = 1")? {
        ExecuteResult::Cursor(cursor) => {
            let batch = db.fetch(&cursor)?;
            println!("rows: {:?}", batch.rows);
        }
        ExecuteResult::CommandComplete {
            affected_rows,
            last_insert_id,
        } => {
            println!(
                "command complete: affected_rows={affected_rows}, last_insert_id={last_insert_id}"
            );
        }
    }

    Ok(())
}
