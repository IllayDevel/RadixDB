use radixdb_client::{Connection, ExecuteResult, WireValue};
use radixdb_orm::{DynamicRecord, Expr, TypedValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .expect("usage: orm <address> <database>");
    let database = std::env::args()
        .nth(2)
        .expect("usage: orm <address> <database>");
    let mut connection = Connection::connect(address)?;
    connection.authenticate("root", None)?;
    connection.select_database(database)?;

    command(connection.execute("DROP TABLE IF EXISTS docs_orm_tasks")?)?;
    command(connection.execute("DROP TABLE IF EXISTS docs_orm_owners")?)?;
    command(connection.execute(
        "CREATE TABLE docs_orm_owners (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
    )?)?;
    command(connection.execute(
        "CREATE TABLE docs_orm_tasks (
            id INTEGER PRIMARY KEY,
            owner_id INTEGER NOT NULL REFERENCES docs_orm_owners(id),
            title TEXT NOT NULL,
            done BOOLEAN NOT NULL DEFAULT false
        )",
    )?)?;

    let owners = connection.entity("docs_orm_owners")?;
    let tasks = connection.entity("docs_orm_tasks")?;

    let mut owner = DynamicRecord::new(owners.descriptor().clone());
    owner.set("id", TypedValue::Integer(1))?;
    owner.set("name", TypedValue::Text("Alice".to_string()))?;
    owner.insert(&mut connection)?;
    assert!(!owner.is_dirty());

    let owner_ref = owners.reference("id", TypedValue::Integer(1))?;
    let mut task = DynamicRecord::new(tasks.descriptor().clone());
    task.set("id", TypedValue::Integer(10))?;
    task.set_reference("owner_id", &owner_ref)?;
    task.set("title", TypedValue::Text("write documentation".to_string()))?;
    task.insert(&mut connection)?;
    assert!(!task.is_dirty());

    task.set("done", TypedValue::Boolean(true))?;
    task.update(&mut connection)?;
    assert!(!task.is_dirty());

    let query = tasks
        .query()
        .select([
            tasks.column("id")?.expr(),
            tasks.column("title")?.expr(),
            tasks.column("done")?.expr(),
        ])
        .filter(tasks.column("owner_id")?.eq(1_i64));
    let ExecuteResult::Cursor(cursor) = query.fetch(&mut connection)? else {
        return Err("ORM query did not open a cursor".into());
    };
    let batch = connection.fetch(&cursor)?;
    assert_eq!(batch.rows.len(), 1);
    assert!(batch.eof);

    connection.begin()?;
    command(connection.execute(
        "INSERT INTO docs_orm_tasks (id, owner_id, title) \
         VALUES (11, 1, 'raw transaction row')",
    )?)?;
    let inside = tasks
        .query()
        .select([Expr::column("id")])
        .filter(Expr::column("id").eq(11_i64));
    let ExecuteResult::Cursor(cursor) = inside.fetch(&mut connection)? else {
        return Err("ORM transaction query did not open a cursor".into());
    };
    assert_eq!(connection.fetch(&cursor)?.rows.len(), 1);
    connection.rollback()?;

    let ExecuteResult::Cursor(cursor) =
        connection.execute("SELECT COUNT(*) FROM docs_orm_tasks WHERE id = 11")?
    else {
        return Err("COUNT did not open a cursor".into());
    };
    let count = connection.fetch(&cursor)?;
    assert_eq!(count.rows[0].values, vec![WireValue::Int(0)]);

    task.delete(&mut connection)?;
    owner.delete(&mut connection)?;
    connection.shutdown()?;
    println!("orm-ok reference=validated crud=verified shared-rollback=verified");
    Ok(())
}

fn command(result: ExecuteResult) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}
