#[cfg(not(feature = "test-mutations"))]
fn main() -> std::process::ExitCode {
    match radixdb::server::run_configured_server_from_env("radixdb-server") {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("radixdb-server: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "test-mutations")]
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == ["--version"] {
        println!("{}", radixdb::server::version_line("radixdb-server"));
        return;
    }
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        println!("{}", radixdb::server::help_text("radixdb-server"));
        return;
    }
    if args.iter().any(|arg| arg == "--version") {
        eprintln!("usage: radixdb-server [--config server.toml] [--print-endpoint] | --version");
        std::process::exit(1);
    }
    eprintln!("test-mutations is forbidden in the production radixdb-server binary");
    std::process::exit(78);
}
