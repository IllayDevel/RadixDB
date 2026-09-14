use std::{env, fs, path::PathBuf, process};

use radixdb_app_sdk::generate_rust_application;
use radixdb_orm::{DatabaseDescriptor, DescriptorEnvelope, DescriptorKind};

fn main() {
    if let Err(error) = run() {
        eprintln!("radixdb-app-codegen: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let descriptor_path = PathBuf::from(arguments.next().ok_or("missing descriptor path")?);
    let output_path = PathBuf::from(arguments.next().ok_or("missing output path")?);
    if arguments.next().is_some() {
        return Err("usage: radixdb-app-codegen <descriptor.json> <output.rs>".into());
    }
    let json = fs::read_to_string(descriptor_path)?;
    let descriptor =
        DescriptorEnvelope::<DatabaseDescriptor>::from_json(&json, DescriptorKind::Database)?;
    let generated = generate_rust_application(&descriptor)?;
    fs::write(output_path, generated.source)?;
    Ok(())
}
