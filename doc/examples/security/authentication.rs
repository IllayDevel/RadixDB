use radixdb_client::{ClientError, Connection, ExecuteResult, WireValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .expect("usage: authentication <address> <database>");
    let database = std::env::args()
        .nth(2)
        .expect("usage: authentication <address> <database>");

    // The test server may use either configured root authentication or the
    // loopback plaintext recovery fallback. Root is used only to provision the
    // durable catalog Principal below.
    let mut root = Connection::connect(&address)?;
    let root_password = std::env::var("RADIXDB_ROOT_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty());
    root.authenticate("root", root_password)?;
    root.select_database(&database)?;
    root.execute("CREATE TABLE auth_probe (id INTEGER PRIMARY KEY)")?;
    root.execute("INSERT INTO auth_probe VALUES (7)")?;
    root.execute("CREATE PRINCIPAL docs_alice PASSWORD 'initial-secret'")?;
    root.execute(format!(
        "GRANT CONNECT ON DATABASE {database} TO docs_alice"
    ))?;
    root.execute("GRANT USAGE ON SCHEMA public TO docs_alice")?;
    root.execute("GRANT SELECT ON TABLE auth_probe TO docs_alice")?;

    let unknown = login_error(&address, &database, "unknown", "initial-secret");
    let wrong = login_error(&address, &database, "docs_alice", "wrong-secret");
    assert_eq!(unknown, wrong, "authentication must not enumerate Principals");

    let mut alice = Connection::connect(&address)?;
    alice.authenticate_database(&database, "docs_alice", "initial-secret")?;
    assert_eq!(scalar_i64(&mut alice, "SELECT id FROM auth_probe")?, 7);

    root.execute("ALTER PRINCIPAL docs_alice DISABLE")?;
    assert_eq!(
        login_error(&address, &database, "docs_alice", "initial-secret"),
        unknown
    );
    // Disable affects new logins; an existing stable Principal session keeps
    // running while its live CONNECT and object privileges remain.
    assert_eq!(scalar_i64(&mut alice, "SELECT id FROM auth_probe")?, 7);

    root.execute("ALTER PRINCIPAL docs_alice ENABLE")?;
    root.execute("ALTER PRINCIPAL docs_alice PASSWORD 'rotated-secret'")?;
    assert_eq!(
        login_error(&address, &database, "docs_alice", "initial-secret"),
        unknown
    );
    let mut rotated = Connection::connect(&address)?;
    rotated.authenticate_database(&database, "docs_alice", "rotated-secret")?;
    assert_eq!(scalar_i64(&mut rotated, "SELECT id FROM auth_probe")?, 7);

    println!(
        "authentication-ok plaintext-password=accepted unknown=indistinguishable \
         disable=new-logins password-rotation=verified"
    );
    Ok(())
}

fn login_error(address: &str, database: &str, login: &str, password: &str) -> String {
    let mut connection = Connection::connect(address).expect("connect authentication probe");
    let error = connection
        .authenticate_database(database, login, password)
        .expect_err("invalid credential unexpectedly authenticated");
    assert_authentication_failed(&error);
    error.to_string()
}

fn scalar_i64(
    connection: &mut Connection,
    sql: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql)? else {
        return Err("query did not return a cursor".into());
    };
    let batch = connection.fetch(&cursor)?;
    let [row] = batch.rows.as_slice() else {
        return Err("query did not return exactly one row".into());
    };
    let [WireValue::Int(value)] = row.values.as_slice() else {
        return Err("query did not return one INTEGER".into());
    };
    Ok(*value)
}

fn assert_authentication_failed(error: &ClientError) {
    let ClientError::Server(failure) = error else {
        panic!("expected server authentication error, got {error}");
    };
    assert_eq!(format!("{:?}", failure.code), "AuthenticationFailed");
}
