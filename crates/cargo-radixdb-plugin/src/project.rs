use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde::Deserialize;

use crate::{command_text, fail, Result};

pub const OFFICIAL_RUSTC: &str = "rustc 1.97.0 (2d8144b78 2026-07-07)";
pub const OFFICIAL_CARGO: &str = "cargo 1.97.0 (c980f4866 2026-06-30)";

#[derive(Debug, Clone)]
pub struct PluginProject {
    pub manifest_path: PathBuf,
    pub target_directory: PathBuf,
    pub lockfile: PathBuf,
    pub package_name: String,
    pub package_version: String,
    pub library_stem: String,
}

#[derive(Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    target_directory: PathBuf,
    packages: Vec<CargoPackage>,
}

#[derive(Deserialize)]
struct CargoPackage {
    name: String,
    version: String,
    manifest_path: PathBuf,
    targets: Vec<CargoTarget>,
    dependencies: Vec<CargoDependency>,
}

#[derive(Deserialize)]
struct CargoTarget {
    name: String,
    crate_types: Vec<String>,
}

#[derive(Deserialize)]
struct CargoDependency {
    name: String,
    kind: Option<String>,
}

pub fn discover(manifest_path: &Path) -> Result<PluginProject> {
    let manifest_path = fs::canonicalize(manifest_path).map_err(|error| {
        fail(format!(
            "cannot canonicalize plugin manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(&manifest_path)
        .output()
        .map_err(|error| fail(format!("cannot execute cargo metadata: {error}")))?;
    require_success("cargo metadata", &output)?;
    let metadata: CargoMetadata = serde_json::from_slice(&output.stdout)
        .map_err(|error| fail(format!("invalid cargo metadata output: {error}")))?;
    let package = metadata
        .packages
        .into_iter()
        .find(|package| package.manifest_path == manifest_path)
        .ok_or_else(|| fail("cargo metadata did not return the requested package"))?;
    let mut cdylibs = package
        .targets
        .iter()
        .filter(|target| target.crate_types.iter().any(|kind| kind == "cdylib"));
    let target = cdylibs
        .next()
        .ok_or_else(|| fail("plugin package must declare exactly one cdylib target"))?;
    if cdylibs.next().is_some() {
        return Err(fail(
            "plugin package must declare exactly one cdylib target",
        ));
    }
    let lockfile = metadata.workspace_root.join("Cargo.lock");
    if !lockfile.is_file() {
        return Err(fail(format!(
            "locked plugin build requires {}",
            lockfile.display()
        )));
    }
    validate_release_profile(&metadata.workspace_root.join("Cargo.toml"))?;
    validate_sdk_dependency(&package.dependencies)?;
    Ok(PluginProject {
        manifest_path,
        target_directory: metadata.target_directory,
        lockfile,
        package_name: package.name,
        package_version: package.version,
        library_stem: target.name.replace('-', "_"),
    })
}

pub fn check(project: &PluginProject, target: &str, target_dir: &Path) -> Result<()> {
    validate_toolchain(target)?;
    let output = Command::new("cargo")
        .args(["check", "--locked", "--release", "--target"])
        .arg(target)
        .args(["--manifest-path"])
        .arg(&project.manifest_path)
        .args(["--target-dir"])
        .arg(target_dir)
        .output()
        .map_err(|error| fail(format!("cannot execute cargo check: {error}")))?;
    require_success("cargo check", &output)
}

pub fn build(project: &PluginProject, target: &str, target_dir: &Path) -> Result<PathBuf> {
    validate_toolchain(target)?;
    let output = Command::new("cargo")
        .args(["rustc", "--locked", "--release", "--target"])
        .arg(target)
        .args(["--manifest-path"])
        .arg(&project.manifest_path)
        .args(["--target-dir"])
        .arg(target_dir)
        .args([
            "--lib",
            "--",
            "-Cpanic=unwind",
            "-Cmetadata=radixdb-plugin-v1",
            "-Clink-arg=-Wl,--build-id=none",
        ])
        .env("SOURCE_DATE_EPOCH", "0")
        .env("CARGO_INCREMENTAL", "0")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .output()
        .map_err(|error| fail(format!("cannot execute cargo rustc: {error}")))?;
    require_success("cargo rustc", &output)?;
    let library = target_dir
        .join(target)
        .join("release")
        .join(format!("lib{}.so", project.library_stem));
    if !library.is_file() {
        return Err(fail(format!(
            "cargo build succeeded but {} is missing",
            library.display()
        )));
    }
    Ok(library)
}

pub fn test(project: &PluginProject) -> Result<()> {
    let output = Command::new("cargo")
        .args(["test", "--locked", "--release", "--manifest-path"])
        .arg(&project.manifest_path)
        .env("SOURCE_DATE_EPOCH", "0")
        .env("CARGO_INCREMENTAL", "0")
        .output()
        .map_err(|error| fail(format!("cannot execute cargo test: {error}")))?;
    require_success("cargo test", &output)
}

pub fn default_target_dir(project: &PluginProject) -> PathBuf {
    project.target_directory.join("radixdb-plugin")
}

pub fn validate_toolchain(target: &str) -> Result<()> {
    let rustc = command_text(Command::new("rustc").arg("--version").arg("--verbose"))?;
    if !rustc.lines().any(|line| line == "release: 1.97.0") {
        return Err(fail("plugin tooling requires rustc 1.97.0"));
    }
    let host = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| fail("rustc did not report its host target"))?;
    if host != target {
        return Err(fail(format!(
            "plugin tooling host target {host} differs from required {target}"
        )));
    }
    Ok(())
}

pub fn tool_versions() -> Result<(String, String)> {
    let rustc = command_text(Command::new("rustc").arg("--version"))?;
    let cargo = command_text(Command::new("cargo").arg("--version"))?;
    Ok((rustc.trim().to_owned(), cargo.trim().to_owned()))
}

pub fn validate_official_tool_versions() -> Result<()> {
    let (rustc, cargo) = tool_versions()?;
    if rustc != OFFICIAL_RUSTC || cargo != OFFICIAL_CARGO {
        return Err(fail(format!(
            "official build requires {OFFICIAL_RUSTC} and {OFFICIAL_CARGO}; found {rustc} and {cargo}"
        )));
    }
    Ok(())
}

fn validate_release_profile(workspace_manifest: &Path) -> Result<()> {
    let source = fs::read_to_string(workspace_manifest).map_err(|error| {
        fail(format!(
            "cannot read workspace manifest {}: {error}",
            workspace_manifest.display()
        ))
    })?;
    let document: toml::Value = toml::from_str(&source)
        .map_err(|error| fail(format!("invalid workspace manifest TOML: {error}")))?;
    let strategy = document
        .get("profile")
        .and_then(|value| value.get("release"))
        .and_then(|value| value.get("panic"))
        .and_then(toml::Value::as_str)
        .unwrap_or("unwind");
    if strategy != "unwind" {
        return Err(fail(
            "release profile must use panic = \"unwind\" for RadixDB plugins",
        ));
    }
    Ok(())
}

fn validate_sdk_dependency(dependencies: &[CargoDependency]) -> Result<()> {
    if !dependencies
        .iter()
        .any(|dependency| dependency.name == "radixdb-plugin" && dependency.kind.is_none())
    {
        return Err(fail(
            "plugin must depend on the public radixdb-plugin crate",
        ));
    }
    if dependencies.iter().any(|dependency| {
        dependency.kind.as_deref() != Some("dev")
            && dependency.name.starts_with("radixdb-")
            && dependency.name != "radixdb-plugin"
    }) {
        return Err(fail(
            "plugin artifact bypasses the public SDK or imports an engine-private owner",
        ));
    }
    Ok(())
}

fn require_success(label: &str, output: &Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    Err(fail(format!(
        "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_a_single_cdylib_with_a_locked_public_sdk_dependency() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml");
        let project = discover(&manifest).unwrap();
        assert_eq!(project.package_name, "radixdb-sdk-proof-plugin");
        assert_eq!(project.library_stem, "radixdb_sdk_proof_plugin");
    }

    #[test]
    fn aborting_release_profile_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("Cargo.toml");
        fs::write(&manifest, "[profile.release]\npanic = \"abort\"\n").unwrap();
        assert!(validate_release_profile(&manifest).is_err());
    }

    #[test]
    fn engine_private_dependencies_are_test_only() {
        let sdk = CargoDependency {
            name: "radixdb-plugin".to_owned(),
            kind: None,
        };
        let test_harness = CargoDependency {
            name: "radixdb-executor".to_owned(),
            kind: Some("dev".to_owned()),
        };
        assert!(validate_sdk_dependency(&[sdk, test_harness]).is_ok());

        let private_artifact_dependency = CargoDependency {
            name: "radixdb-storage".to_owned(),
            kind: None,
        };
        assert!(validate_sdk_dependency(&[
            CargoDependency {
                name: "radixdb-plugin".to_owned(),
                kind: None,
            },
            private_artifact_dependency,
        ])
        .is_err());
    }
}
