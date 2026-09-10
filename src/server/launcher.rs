use std::{
    fs,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use radixdb_plugin_host::load_plugin_registry;
use signal_hook::consts::{SIGINT, SIGTERM};

use super::{parse_server_config_file, version_line, Server};

#[derive(Debug, Clone, PartialEq, Eq)]
enum LauncherAction {
    Help,
    Version,
    Run {
        config_path: PathBuf,
        print_endpoint: bool,
    },
}

pub fn help_text(process_name: &str) -> String {
    format!(
        "{process_name} — RadixDB TCP server\n\n\
Usage:\n  {process_name} [--config PATH] [--print-endpoint]\n  {process_name} --version\n  {process_name} --help\n\n\
Options:\n  --config PATH       Read configuration from PATH (default: server.toml).\n  --print-endpoint    Read and validate configuration, print BIND_IP PORT, then exit without listening.\n  --version           Print build and protocol identity, then exit.\n  -h, --help          Print this help, then exit."
    )
}

pub fn run_configured_server_from_env(
    process_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    run_configured_server(std::env::args().skip(1), process_name)
}

pub fn run_configured_server(
    args: impl Iterator<Item = String>,
    process_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = parse_config_args(args.into_iter(), process_name)?;
    let (config_path, print_endpoint) = match action {
        LauncherAction::Help => {
            println!("{}", help_text(process_name));
            return Ok(());
        }
        LauncherAction::Version => {
            println!("{}", version_line(process_name));
            return Ok(());
        }
        LauncherAction::Run {
            config_path,
            print_endpoint,
        } => (config_path, print_endpoint),
    };
    let config_source = fs::read_to_string(&config_path)?;
    let config = parse_server_config_file(&config_source)
        .map_err(|error| format!("failed to parse {}: {error}", config_path.display()))?;
    if print_endpoint {
        println!("{} {}", config.server.bind_ip, config.server.port);
        return Ok(());
    }
    let plugin_registry = load_plugin_registry(&config.plugins)?;
    let plugin_status = plugin_registry.status();
    eprintln!(
        "{process_name} plugin registry generation={} packages={} types={} functions={} operators={} operator_classes={} planner_support={} shadowed_versions={} library_bytes={}",
        plugin_status.generation,
        plugin_status.packages,
        plugin_status.external_types,
        plugin_status.functions,
        plugin_status.operators,
        plugin_status.operator_classes,
        plugin_status.planner_support,
        plugin_status.shadowed_versions,
        plugin_status.loaded_library_bytes,
    );
    let server = Server::bind_with_plugin_registry(&config.server, plugin_registry)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGTERM, Arc::clone(&shutdown))?;
    signal_hook::flag::register(SIGINT, Arc::clone(&shutdown))?;
    eprintln!("{process_name} listening on {}", server.local_addr()?);
    server.run_until(&shutdown)?;
    eprintln!("{process_name} stopped cleanly");
    Ok(())
}

fn parse_config_args(
    mut args: impl Iterator<Item = String>,
    process_name: &str,
) -> Result<LauncherAction, String> {
    let first = args.next();
    let second = args.next();
    let third = args.next();
    let fourth = args.next();
    match (first, second, third, fourth) {
        (None, None, None, None) => Ok(LauncherAction::Run {
            config_path: PathBuf::from("server.toml"),
            print_endpoint: false,
        }),
        (Some(help), None, None, None) if help == "--help" || help == "-h" => {
            Ok(LauncherAction::Help)
        }
        (Some(version), None, None, None) if version == "--version" => Ok(LauncherAction::Version),
        (Some(print), None, None, None) if print == "--print-endpoint" => Ok(LauncherAction::Run {
            config_path: PathBuf::from("server.toml"),
            print_endpoint: true,
        }),
        (Some(flag), Some(path), None, None) if flag == "--config" => Ok(LauncherAction::Run {
            config_path: PathBuf::from(path),
            print_endpoint: false,
        }),
        (Some(flag), Some(path), Some(print), None)
            if flag == "--config" && print == "--print-endpoint" =>
        {
            Ok(LauncherAction::Run {
                config_path: PathBuf::from(path),
                print_endpoint: true,
            })
        }
        (Some(print), Some(flag), Some(path), None)
            if print == "--print-endpoint" && flag == "--config" =>
        {
            Ok(LauncherAction::Run {
                config_path: PathBuf::from(path),
                print_endpoint: true,
            })
        }
        _ => Err(format!(
            "usage: {process_name} [--config PATH] [--print-endpoint] | --version | --help"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<LauncherAction, String> {
        parse_config_args(
            args.iter().map(|argument| (*argument).to_string()),
            "radixdb-server",
        )
    }

    #[test]
    fn help_and_version_are_terminal_success_actions_without_config_io() {
        assert_eq!(parse(&["--help"]), Ok(LauncherAction::Help));
        assert_eq!(parse(&["-h"]), Ok(LauncherAction::Help));
        assert_eq!(parse(&["--version"]), Ok(LauncherAction::Version));
        assert!(help_text("radixdb-server").contains("default: server.toml"));
        assert!(help_text("radixdb-server").contains("without listening"));
        run_configured_server(["--help".to_string()].into_iter(), "radixdb-server")
            .expect("help must not read the absent default configuration");
    }

    #[test]
    fn config_and_print_endpoint_accept_both_orders_and_reject_conflicts() {
        let expected = LauncherAction::Run {
            config_path: PathBuf::from("custom.toml"),
            print_endpoint: true,
        };
        assert_eq!(
            parse(&["--config", "custom.toml", "--print-endpoint"]),
            Ok(expected.clone())
        );
        assert_eq!(
            parse(&["--print-endpoint", "--config", "custom.toml"]),
            Ok(expected)
        );
        assert_eq!(
            parse(&["--print-endpoint"]),
            Ok(LauncherAction::Run {
                config_path: PathBuf::from("server.toml"),
                print_endpoint: true,
            })
        );
        for arguments in [
            &["--help", "--version"][..],
            &["--config"][..],
            &["--config", "a", "--config", "b"][..],
            &["--print-endpoint", "--print-endpoint"][..],
            &["--unknown"][..],
        ] {
            assert!(parse(arguments).is_err(), "must reject {arguments:?}");
        }
    }
}
