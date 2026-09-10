use std::path::PathBuf;

use clap::Parser;
use radixdb_soak::{build_identity, config::SoakConfig, diagnostics::observer};

#[derive(Debug, Parser)]
#[command(name = "radixdb-soak-observer", disable_version_flag = true)]
struct Cli {
    #[arg(long)]
    version: bool,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, hide = true)]
    max_samples: Option<u64>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("radixdb-soak-observer: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.version {
        println!("{} component=observer", build_identity());
        return Ok(());
    }
    let config = cli.config.ok_or("--config is required")?;
    let run_id = cli.run_id.ok_or("--run-id is required")?;
    observer::run(
        SoakConfig::load(&config)?.resolve()?,
        &run_id,
        cli.max_samples,
    )?;
    Ok(())
}
