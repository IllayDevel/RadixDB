mod support;

use radixdb_client::{Connection, ExecuteResult, TransactionState};
use radixdb_orm::{
    table, Column, DataTypeDescriptor, DdlBuilder, Expr, InsertBuilder, OrmBuilder, QueryBuilder,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let create = DdlBuilder::create_table("people")
        .if_not_exists(true)
        .column(
            Column::new("id", DataTypeDescriptor::Integer)
                .not_null(true)
                .primary_key(true),
        )
        .column(Column::text("name").not_null(true));

    let insert = InsertBuilder::new("people")
        .value("id", 7_i64)
        .value("name", "Alice")
        .returning_all();

    let query = QueryBuilder::from_relation(table("people"))
        .select([Expr::column("id"), Expr::column("name")])
        .filter(Expr::column("id").eq(7_i64));

    for (label, json, sql) in [
        ("create", create.to_json()?, create.to_sql()?),
        ("insert", insert.to_json()?, insert.to_sql()?),
        ("query", query.to_json()?, query.to_sql()?),
    ] {
        println!("{label} JSON:\n{json}");
        println!("{label} SQL: {}", sql.sql);
        println!("{label} parameters: {:?}", sql.parameters);
    }

    if std::env::args().nth(1).as_deref() == Some("--transaction-smoke") {
        let mut connection = support::connect()?;
        raw_and_orm_same_connection(&mut connection)?;
        println!("raw SQL + ORM transaction contract passed");
    }
    Ok(())
}

/// Runtime contract: raw SQL and ORM borrow the same transport and transaction
/// owner. The public smoke invokes this after authentication/database selection.
fn raw_and_orm_same_connection(
    connection: &mut Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    const TABLE: &str = "orm_transaction_contract_people";
    connection.execute(format!(
        "CREATE TABLE IF NOT EXISTS {TABLE} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
    ))?;
    connection.execute(format!("DELETE FROM {TABLE}"))?;

    connection.begin()?;
    assert_eq!(connection.transaction_state(), TransactionState::Active);
    connection.execute(format!("INSERT INTO {TABLE} (id, name) VALUES (1, 'raw')"))?;

    let query = QueryBuilder::from_relation(table(TABLE))
        .select([Expr::star()])
        .filter(Expr::column("id").eq(1_i64));
    let ExecuteResult::Cursor(cursor) = QueryBuilder::from_relation(table(TABLE))
        .select([Expr::star()])
        .filter(Expr::column("id").eq(1_i64))
        .fetch(&mut *connection)?
    else {
        return Err("ORM SELECT did not return a cursor".into());
    };
    assert_eq!(support::fetch_all(connection, &cursor)?.len(), 1);
    connection.rollback()?;
    assert_eq!(connection.transaction_state(), TransactionState::Inactive);

    let ExecuteResult::Cursor(cursor) = query.fetch(&mut *connection)? else {
        return Err("ORM SELECT after rollback did not return a cursor".into());
    };
    assert!(support::fetch_all(connection, &cursor)?.is_empty());

    connection.begin()?;
    connection.execute(format!("INSERT INTO {TABLE} (id, name) VALUES (2, 'committed')"))?;
    connection.commit()?;
    assert_eq!(connection.transaction_state(), TransactionState::Inactive);

    connection.begin()?;
    let duplicate = connection.execute(format!(
        "INSERT INTO {TABLE} (id, name) VALUES (2, 'duplicate')"
    ));
    assert!(duplicate.is_err(), "duplicate primary key must fail");
    assert_eq!(
        connection.transaction_state(),
        TransactionState::Active,
        "a statement error must keep the explicit transaction available for rollback"
    );
    connection.rollback()?;
    assert_eq!(connection.transaction_state(), TransactionState::Inactive);

    let committed = QueryBuilder::from_relation(table(TABLE))
        .select([Expr::star()])
        .filter(Expr::column("id").eq(2_i64));
    let ExecuteResult::Cursor(cursor) = committed.fetch(&mut *connection)? else {
        return Err("ORM SELECT after commit did not return a cursor".into());
    };
    assert_eq!(support::fetch_all(connection, &cursor)?.len(), 1);
    Ok(())
}
