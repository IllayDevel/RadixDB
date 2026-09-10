mod support;

use std::path::PathBuf;

use radixdb_orm::{DescriptorEnvelope, DescriptorKind};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema/radixtrade.schema.json")
        });

    let mut connection = support::connect()?;
    let descriptor = connection.schema().describe_database().fetch()?;
    let fingerprint = descriptor.fingerprint.clone();
    let json = DescriptorEnvelope::new(DescriptorKind::Database, descriptor).to_pretty_json()?;

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output, json)?;
    println!("descriptor: {}", output.display());
    println!("fingerprint: {fingerprint}");
    connection.shutdown()?;
    Ok(())
}
