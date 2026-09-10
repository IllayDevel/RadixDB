#![forbid(unsafe_op_in_unsafe_fn)]

use std::{
    error::Error,
    ffi::OsString,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    process::Command,
};

use clap::{ArgGroup, Args, Parser, Subcommand};
use sha2::{Digest, Sha256};

mod golden;
mod inspect;
mod package;
mod project;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(name = "cargo radixdb-plugin", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate the project contract and run a locked release check.
    Check(ProjectArgs),
    /// Build a deterministic release cdylib with unwind containment.
    Build(ProjectArgs),
    /// Inspect a raw library or verify a complete package.
    Inspect(InspectArgs),
    /// Run package tests and validate codec golden vectors in an isolated host.
    TestHost(ProjectArgs),
    /// Build and atomically create a complete host-admissible package.
    Package(PackageArgs),
    #[command(name = "__inspect-child", hide = true)]
    InspectChild {
        #[arg(long)]
        library: PathBuf,
    },
    #[command(name = "__golden-child", hide = true)]
    GoldenChild {
        #[arg(long)]
        library: PathBuf,
        #[arg(long)]
        golden: PathBuf,
    },
    #[command(name = "__admission-child", hide = true)]
    AdmissionChild {
        #[arg(long)]
        package: PathBuf,
    },
}

#[derive(Debug, Clone, Args)]
struct ProjectArgs {
    #[arg(long, default_value = "Cargo.toml")]
    manifest_path: PathBuf,
    #[arg(long)]
    target_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("input")
        .required(true)
        .multiple(false)
        .args(["library", "package"])
))]
struct InspectArgs {
    #[arg(long)]
    library: Option<PathBuf>,
    #[arg(long)]
    package: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PackageArgs {
    #[arg(long, default_value = "Cargo.toml")]
    manifest_path: PathBuf,
    #[arg(long)]
    target_dir: Option<PathBuf>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    golden: Option<PathBuf>,
    #[arg(long)]
    previous_package: Option<PathBuf>,
}

pub fn run() -> Result<()> {
    let arguments = normalized_arguments();
    let cli = Cli::parse_from(arguments);
    let executable = std::env::current_exe()
        .map_err(|error| fail(format!("cannot resolve tooling executable: {error}")))?;
    match cli.command {
        Commands::Check(arguments) => {
            let (project, target_dir) = resolve_project(&arguments)?;
            project::check(&project, radixdb_plugin_host::SUPPORTED_TARGET, &target_dir)?;
            println!(
                "plugin check passed: {} {}",
                project.package_name, project.package_version
            );
        }
        Commands::Build(arguments) => {
            let (project, target_dir) = resolve_project(&arguments)?;
            let library =
                project::build(&project, radixdb_plugin_host::SUPPORTED_TARGET, &target_dir)?;
            let report = inspect::inspect_isolated(&executable, &library)?;
            println!("{}", library.display());
            eprintln!(
                "built {} {} with descriptor {}",
                report.name, report.version, report.descriptor_fingerprint
            );
        }
        Commands::Inspect(arguments) => {
            let report = if let Some(library) = arguments.library {
                let library = canonical_file(&library)?;
                inspect::inspect_isolated(&executable, &library)?
            } else {
                package::verify_package(
                    &executable,
                    arguments.package.as_deref().expect("clap input group"),
                )?
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .map_err(|error| fail(format!("cannot encode inspection report: {error}")))?
            );
        }
        Commands::TestHost(arguments) => {
            let (project, target_dir) = resolve_project(&arguments)?;
            project::test(&project)?;
            let library =
                project::build(&project, radixdb_plugin_host::SUPPORTED_TARGET, &target_dir)?;
            let report = inspect::inspect_isolated(&executable, &library)?;
            let golden = default_golden(&project);
            golden::validate_isolated(&executable, &library, &golden)?;
            println!(
                "local host passed: {} types={} functions={}",
                report.name,
                report.types.len(),
                report.functions.len()
            );
        }
        Commands::Package(arguments) => {
            package::require_official_environment()?;
            let project_arguments = ProjectArgs {
                manifest_path: arguments.manifest_path,
                target_dir: arguments.target_dir,
            };
            let (project, target_dir) = resolve_project(&project_arguments)?;
            project::test(&project)?;
            let library =
                project::build(&project, radixdb_plugin_host::SUPPORTED_TARGET, &target_dir)?;
            let golden = arguments.golden.unwrap_or_else(|| default_golden(&project));
            let output = package::create(package::PackageRequest {
                executable: &executable,
                project: &project,
                library: &library,
                output_directory: &arguments.output_dir,
                golden_path: &golden,
                previous_package: arguments.previous_package.as_deref(),
            })?;
            println!("{}", output.display());
        }
        Commands::InspectChild { library } => {
            let report = inspect::inspect_child(&library)?;
            serde_json::to_writer(io::stdout().lock(), &report)
                .map_err(|error| fail(format!("cannot write inspection report: {error}")))?;
        }
        Commands::GoldenChild { library, golden } => {
            golden::validate_child(&library, &golden)?;
        }
        Commands::AdmissionChild { package } => {
            package::admission_child(&package)?;
        }
    }
    Ok(())
}

fn resolve_project(arguments: &ProjectArgs) -> Result<(project::PluginProject, PathBuf)> {
    let project = project::discover(&arguments.manifest_path)?;
    let target_dir = arguments
        .target_dir
        .clone()
        .unwrap_or_else(|| project::default_target_dir(&project));
    Ok((project, target_dir))
}

fn default_golden(project: &project::PluginProject) -> PathBuf {
    project
        .manifest_path
        .parent()
        .expect("manifest is canonical")
        .join(golden::GOLDEN_FILE)
}

fn normalized_arguments() -> Vec<OsString> {
    let mut arguments = std::env::args_os().collect::<Vec<_>>();
    if arguments
        .get(1)
        .is_some_and(|value| value == "radixdb-plugin")
    {
        arguments.remove(1);
    }
    arguments
}

fn canonical_file(path: &Path) -> Result<PathBuf> {
    let path = std::fs::canonicalize(path)
        .map_err(|error| fail(format!("cannot canonicalize {}: {error}", path.display())))?;
    if !path.is_file() {
        return Err(fail(format!("not a regular file: {}", path.display())));
    }
    Ok(path)
}

pub(crate) fn command_text(command: &mut Command) -> Result<String> {
    let output = command
        .output()
        .map_err(|error| fail(format!("cannot execute command: {error}")))?;
    if !output.status.success() {
        return Err(fail(format!(
            "command failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| fail(format!("command output is not UTF-8: {error}")))
}

pub(crate) fn isolated_command(executable: &Path) -> Command {
    let mut command = Command::new("timeout");
    command
        .args(["--signal=KILL", "--kill-after=1s", "15s"])
        .arg(executable);
    command
}

pub(crate) fn sha256_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path)
        .map_err(|error| fail(format!("cannot open {}: {error}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| fail(format!("cannot hash {}: {error}", path.display())))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn fail(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_hex_are_stable() {
        let temporary = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temporary.path(), b"radixdb").unwrap();
        assert_eq!(
            hex(&sha256_file(temporary.path()).unwrap()),
            "b4b9e4d1b3920cdaf95f9748029ad39008fadea7375b8935ce0b0633cf4503f5"
        );
    }
}
