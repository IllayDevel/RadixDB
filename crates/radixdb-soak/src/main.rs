use std::{
    fs::File,
    io::{self, BufReader},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use radixdb_soak::{
    build_identity, comparison,
    config::SoakConfig,
    diagnostics::{read_frames, replay, DetectorConfig},
    runner,
};

#[derive(Debug, Parser)]
#[command(name = "radixdb-soak", disable_version_flag = true)]
struct Cli {
    #[arg(long)]
    version: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        #[arg(long)]
        config: PathBuf,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        run_id: String,
    },
    Seed {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        run_id: String,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 60_000)]
        workload_stall_millis: u64,
        #[arg(long, default_value_t = 30_000)]
        agent_heartbeat_timeout_millis: u64,
    },
    Summarize {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        json: PathBuf,
        #[arg(long)]
        markdown: PathBuf,
    },
    Compare {
        #[arg(long)]
        pair_id: String,
        #[arg(long)]
        radixdb_run: PathBuf,
        #[arg(long)]
        postgresql_run: PathBuf,
        #[arg(long)]
        json: PathBuf,
        #[arg(long)]
        markdown: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("radixdb-soak: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", build_identity());
        return Ok(());
    }
    match cli.command {
        Some(Command::Validate { config }) => {
            let resolved = SoakConfig::load(&config)?.resolve()?;
            println!(
                "valid profile={} duration_secs={} database={} status={}",
                resolved.profile.as_str(),
                resolved.duration.as_secs(),
                resolved.database.address,
                resolved.status.bind
            );
            Ok(())
        }
        Some(Command::Run { config, run_id }) => {
            let resolved = SoakConfig::load(&config)?.resolve()?;
            runner::run(resolved, &run_id)?;
            Ok(())
        }
        Some(Command::Seed { config, run_id }) => {
            let resolved = SoakConfig::load(&config)?.resolve()?;
            runner::seed(resolved, &run_id)?;
            Ok(())
        }
        Some(Command::Replay {
            input,
            workload_stall_millis,
            agent_heartbeat_timeout_millis,
        }) => {
            let frames = read_frames(BufReader::new(File::open(input)?))?;
            let alerts = replay(
                &frames,
                DetectorConfig {
                    workload_stall_millis,
                    agent_heartbeat_timeout_millis,
                },
            )?;
            serde_json::to_writer_pretty(io::stdout().lock(), &alerts)?;
            println!();
            Ok(())
        }
        Some(Command::Summarize {
            run_dir,
            json,
            markdown,
        }) => {
            let summary = comparison::write_run_summary(&run_dir, &json, &markdown)?;
            println!(
                "summarized run={} engine={} samples={}",
                summary.run_id,
                summary.database_engine.as_str(),
                summary.sample_count
            );
            Ok(())
        }
        Some(Command::Compare {
            pair_id,
            radixdb_run,
            postgresql_run,
            json,
            markdown,
        }) => {
            let report = comparison::write_comparison(
                &pair_id,
                &radixdb_run,
                &postgresql_run,
                &json,
                &markdown,
            )?;
            println!(
                "compared radixdb={} postgresql={} metrics={}",
                report.radixdb.run_id,
                report.postgresql.run_id,
                report.metrics.len()
            );
            Ok(())
        }
        None => Err("expected --version or a subcommand".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_subcommand_requires_config_and_run_id() {
        let cli = Cli::try_parse_from([
            "radixdb-soak",
            "seed",
            "--config",
            "soak.toml",
            "--run-id",
            "icp-6-6",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Seed { config, run_id })
                if config.as_path() == std::path::Path::new("soak.toml")
                    && run_id == "icp-6-6"
        ));
    }
}
