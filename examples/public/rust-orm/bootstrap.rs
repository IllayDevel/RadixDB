mod support;

const RADIXTRADE_SCHEMA: &str = include_str!("../radixtrade/schema.sql");
const RADIXTRADE_SEED: &str = include_str!("../radixtrade/seed-small.sql");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = support::connect()?;
    support::execute_script(&mut connection, "schema", RADIXTRADE_SCHEMA)?;
    support::execute_script(&mut connection, "seed", RADIXTRADE_SEED)?;

    println!("RadixTrade schema and deterministic seed are ready");
    connection.shutdown()?;
    Ok(())
}
