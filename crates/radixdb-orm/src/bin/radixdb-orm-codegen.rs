use std::fs;
use std::path::PathBuf;

use radixdb_orm::{generate_rust_database, DatabaseDescriptor, DescriptorEnvelope, DescriptorKind};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let input = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: radixdb-orm-codegen <descriptor.json> <output.rs>")?;
    let output = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: radixdb-orm-codegen <descriptor.json> <output.rs>")?;
    if arguments.next().is_some() {
        return Err("usage: radixdb-orm-codegen <descriptor.json> <output.rs>".into());
    }
    let json = fs::read_to_string(&input)?;
    let descriptor =
        DescriptorEnvelope::<DatabaseDescriptor>::from_json(&json, DescriptorKind::Database)?;
    let generated = generate_rust_database(&descriptor)?;
    fs::write(output, generated.source)?;
    Ok(())
}
