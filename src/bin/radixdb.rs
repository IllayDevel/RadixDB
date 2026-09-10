//! Thin process entrypoint for the RadixDB CLI runtime.

fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(radixdb::cli::run_from_env())
}
