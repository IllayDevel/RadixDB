use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use radixdb_plugin_host::{
    load_plugin_registry, PluginHostConfig, PluginPackageManifest, MANIFEST_FORMAT,
    MAXIMUM_REQUIRED_GLIBC, OFFICIAL_BUILD_IMAGE, PLUGIN_MANIFEST_FILE, SUPPORTED_TARGET,
};
use serde::{Deserialize, Serialize};

use crate::{
    fail,
    golden::{self, CodecGoldenFile, GOLDEN_FILE},
    hex,
    inspect::{self, DescriptorReport},
    isolated_command,
    project::{self, PluginProject},
    sha256_file, Result,
};

pub const PROVENANCE_FILE: &str = "radixdb-plugin-provenance.toml";
pub const COMPATIBILITY_FILE: &str = "radixdb-plugin-compatibility.toml";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildProvenance {
    pub format: u16,
    pub tool: String,
    pub rustc: String,
    pub cargo: String,
    pub build_image: String,
    pub target: String,
    pub panic_strategy: String,
    pub manifest_sha256: String,
    pub source_manifest_sha256: String,
    pub source_lock_sha256: String,
    pub library_sha256: String,
    pub descriptor_fingerprint: String,
    pub codec_golden_sha256: String,
    pub compatibility_report_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompatibilityReport {
    pub format: u16,
    pub package_id: String,
    pub previous_version: Option<String>,
    pub current_version: String,
    pub compatible: bool,
    pub findings: Vec<String>,
}

pub struct PackageRequest<'a> {
    pub executable: &'a Path,
    pub project: &'a PluginProject,
    pub library: &'a Path,
    pub output_directory: &'a Path,
    pub golden_path: &'a Path,
    pub previous_package: Option<&'a Path>,
}

pub fn create(request: PackageRequest<'_>) -> Result<PathBuf> {
    require_official_environment()?;
    let report = inspect::inspect_isolated(request.executable, request.library)?;
    if report.version != request.project.package_version {
        return Err(fail(format!(
            "Cargo package version {} differs from embedded descriptor version {}",
            request.project.package_version, report.version
        )));
    }
    let golden = golden::read(request.golden_path)?;
    validate_golden_metadata(&report, &golden)?;
    golden::validate_isolated(request.executable, request.library, request.golden_path)?;
    let compatibility =
        compatibility_report(request.executable, &report, request.previous_package)?;
    if !compatibility.compatible {
        return Err(fail(format!(
            "package is incompatible with the requested predecessor: {}",
            compatibility.findings.join("; ")
        )));
    }

    let output = absolute_new_path(request.output_directory)?;
    let parent = output
        .parent()
        .ok_or_else(|| fail("package output has no parent directory"))?;
    let stage = parent.join(format!(
        ".radixdb-plugin-stage-{}-{}",
        std::process::id(),
        report.name
    ));
    if stage.exists() {
        return Err(fail(format!(
            "private staging directory already exists: {}",
            stage.display()
        )));
    }
    fs::create_dir(&stage)
        .map_err(|error| fail(format!("cannot create package staging directory: {error}")))?;
    set_mode(&stage, 0o755)?;
    let result = create_staged_package(
        &stage,
        request.project,
        request.library,
        request.golden_path,
        &report,
        &compatibility,
    );
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&stage);
        return Err(error);
    }
    if let Err(error) = admit_isolated(request.executable, &stage) {
        let _ = fs::remove_dir_all(&stage);
        return Err(error);
    }
    fs::rename(&stage, &output)
        .map_err(|error| fail(format!("cannot publish package directory: {error}")))?;
    sync_directory(parent)?;
    Ok(output)
}

pub fn verify_package(executable: &Path, directory: &Path) -> Result<DescriptorReport> {
    let directory = fs::canonicalize(directory).map_err(|error| {
        fail(format!(
            "cannot canonicalize package {}: {error}",
            directory.display()
        ))
    })?;
    let manifest = read_manifest(&directory)?;
    validate_package_layout(&directory, &manifest)?;
    admit_isolated(executable, &directory)?;
    let library = directory.join(&manifest.library);
    let report = inspect::inspect_isolated(executable, &library)?;
    let golden_path = directory.join(GOLDEN_FILE);
    let golden = golden::read(&golden_path)?;
    validate_golden_metadata(&report, &golden)?;
    golden::validate_isolated(executable, &library, &golden_path)?;
    verify_provenance(&directory, &manifest, &report)?;
    Ok(report)
}

fn validate_package_layout(directory: &Path, manifest: &PluginPackageManifest) -> Result<()> {
    let mut root_entries = fs::read_dir(directory)
        .map_err(|error| fail(format!("cannot list package directory: {error}")))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|error| fail(format!("cannot read package directory entry: {error}")))
        })
        .collect::<Result<Vec<_>>>()?;
    root_entries.sort();
    let mut expected = vec![
        std::ffi::OsString::from(COMPATIBILITY_FILE),
        std::ffi::OsString::from(GOLDEN_FILE),
        std::ffi::OsString::from(PROVENANCE_FILE),
        std::ffi::OsString::from(PLUGIN_MANIFEST_FILE),
        std::ffi::OsString::from("lib"),
    ];
    expected.sort();
    if root_entries != expected {
        return Err(fail(format!(
            "package root must contain exactly the declared v1 files; found {root_entries:?}"
        )));
    }
    validate_plain_path(&directory.join(GOLDEN_FILE), false)?;
    validate_plain_path(&directory.join(PROVENANCE_FILE), false)?;
    validate_plain_path(&directory.join(COMPATIBILITY_FILE), false)?;
    let library_directory = directory.join("lib");
    validate_plain_path(&library_directory, true)?;
    let libraries = fs::read_dir(&library_directory)
        .map_err(|error| fail(format!("cannot list package library directory: {error}")))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| fail(format!("cannot read package library entry: {error}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let expected_library = directory.join(&manifest.library);
    if libraries.len() != 1 || libraries[0] != expected_library {
        return Err(fail(
            "package lib directory must contain exactly the manifest library",
        ));
    }
    Ok(())
}

fn validate_plain_path(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| fail(format!("cannot inspect {}: {error}", path.display())))?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(fail(format!(
            "package path has an invalid file type: {}",
            path.display()
        )));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o022 != 0 {
        return Err(fail(format!(
            "package path allows group/world writes: {}",
            path.display()
        )));
    }
    Ok(())
}

pub fn admission_child(directory: &Path) -> Result<()> {
    let directory = fs::canonicalize(directory).map_err(|error| {
        fail(format!(
            "cannot canonicalize package {}: {error}",
            directory.display()
        ))
    })?;
    let registry = load_plugin_registry(&PluginHostConfig {
        package_directories: vec![directory],
    })
    .map_err(|error| fail(format!("PLUG-20 admission rejected package: {error}")))?;
    if registry.status().packages != 1 {
        return Err(fail(
            "PLUG-20 admission did not publish exactly one package",
        ));
    }
    Ok(())
}

fn create_staged_package(
    stage: &Path,
    project: &PluginProject,
    source_library: &Path,
    golden_path: &Path,
    report: &DescriptorReport,
    compatibility: &CompatibilityReport,
) -> Result<()> {
    let library_directory = stage.join("lib");
    fs::create_dir(&library_directory)
        .map_err(|error| fail(format!("cannot create package lib directory: {error}")))?;
    set_mode(&library_directory, 0o755)?;
    let library_name = format!("lib{}.so", library_component(&report.name));
    let library_path = library_directory.join(&library_name);
    fs::copy(source_library, &library_path)
        .map_err(|error| fail(format!("cannot copy plugin library: {error}")))?;
    set_mode(&library_path, 0o644)?;
    let library_sha256 = hex(&sha256_file(&library_path)?);
    let glibc = inspect::maximum_required_glibc(&library_path)?;
    validate_glibc_ceiling(&glibc)?;
    let manifest = PluginPackageManifest {
        format: MANIFEST_FORMAT,
        package_id: report.package_id.clone(),
        name: report.name.clone(),
        version: semver::Version::parse(&report.version)
            .map_err(|error| fail(format!("invalid descriptor version: {error}")))?,
        library: PathBuf::from(format!("lib/{library_name}")),
        library_sha256: library_sha256.clone(),
        descriptor_fingerprint: report.descriptor_fingerprint.clone(),
        target: SUPPORTED_TARGET.to_owned(),
        maximum_required_glibc: glibc,
        build_image: OFFICIAL_BUILD_IMAGE.to_owned(),
        panic_strategy: "unwind".to_owned(),
        abi_major: report.abi_major,
        abi_min_minor: report.abi_min_minor,
        abi_max_minor: report.abi_max_minor,
    };
    manifest
        .validate_static_fields()
        .map_err(|error| fail(format!("generated manifest is invalid: {error}")))?;
    write_toml(&stage.join(PLUGIN_MANIFEST_FILE), &manifest)?;

    let packaged_golden = stage.join(GOLDEN_FILE);
    fs::copy(golden_path, &packaged_golden)
        .map_err(|error| fail(format!("cannot copy codec golden file: {error}")))?;
    set_mode(&packaged_golden, 0o644)?;
    write_toml(&stage.join(COMPATIBILITY_FILE), compatibility)?;
    let compatibility_report_sha256 = hex(&sha256_file(&stage.join(COMPATIBILITY_FILE))?);
    let (rustc, cargo) = project::tool_versions()?;
    let provenance = BuildProvenance {
        format: 1,
        tool: format!("cargo-radixdb-plugin {}", env!("CARGO_PKG_VERSION")),
        rustc,
        cargo,
        build_image: OFFICIAL_BUILD_IMAGE.to_owned(),
        target: SUPPORTED_TARGET.to_owned(),
        panic_strategy: "unwind".to_owned(),
        manifest_sha256: hex(&sha256_file(&stage.join(PLUGIN_MANIFEST_FILE))?),
        source_manifest_sha256: hex(&sha256_file(&project.manifest_path)?),
        source_lock_sha256: hex(&sha256_file(&project.lockfile)?),
        library_sha256,
        descriptor_fingerprint: report.descriptor_fingerprint.clone(),
        codec_golden_sha256: hex(&sha256_file(&packaged_golden)?),
        compatibility_report_sha256,
    };
    write_toml(&stage.join(PROVENANCE_FILE), &provenance)?;
    sync_file(&library_path)?;
    sync_file(&packaged_golden)?;
    sync_file(&stage.join(PLUGIN_MANIFEST_FILE))?;
    sync_file(&stage.join(PROVENANCE_FILE))?;
    sync_file(&stage.join(COMPATIBILITY_FILE))?;
    sync_directory(&library_directory)?;
    sync_directory(stage)
}

fn compatibility_report(
    executable: &Path,
    current: &DescriptorReport,
    previous_package: Option<&Path>,
) -> Result<CompatibilityReport> {
    let Some(previous_package) = previous_package else {
        return Ok(CompatibilityReport {
            format: 1,
            package_id: current.package_id.clone(),
            previous_version: None,
            current_version: current.version.clone(),
            compatible: true,
            findings: vec!["initial package baseline; no predecessor requested".to_owned()],
        });
    };
    let previous = verify_package(executable, previous_package)?;
    let mut findings = Vec::new();
    let mut compatible = true;
    if previous.package_id != current.package_id || previous.name != current.name {
        compatible = false;
        findings.push("package identity or canonical name changed".to_owned());
    }
    let previous_version = semver::Version::parse(&previous.version)
        .map_err(|error| fail(format!("invalid previous package version: {error}")))?;
    let current_version = semver::Version::parse(&current.version)
        .map_err(|error| fail(format!("invalid current package version: {error}")))?;
    if current_version <= previous_version {
        compatible = false;
        findings.push("current package version must be greater than predecessor".to_owned());
    }
    let current_types = current
        .types
        .iter()
        .map(|value| (value.object_id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    for old in &previous.types {
        let Some(new) = current_types.get(old.object_id.as_str()) else {
            compatible = false;
            findings.push(format!("external type {} was removed", old.local_id));
            continue;
        };
        if new.codec_version < old.codec_version {
            compatible = false;
            findings.push(format!("type {} codec version regressed", old.local_id));
        } else if new.codec_version == old.codec_version
            && new.codec_fingerprint != old.codec_fingerprint
        {
            compatible = false;
            findings.push(format!(
                "type {} changed codec fingerprint without a codec version bump",
                old.local_id
            ));
        }
        if new.semantic_revision < old.semantic_revision {
            compatible = false;
            findings.push(format!("type {} semantic revision regressed", old.local_id));
        }
    }
    compare_revisions(
        "function",
        &previous.functions,
        &current.functions,
        &mut compatible,
        &mut findings,
    );
    compare_revisions(
        "operator",
        &previous.operators,
        &current.operators,
        &mut compatible,
        &mut findings,
    );
    compare_fingerprinted_revisions(
        "operator class",
        &previous.operator_classes,
        &current.operator_classes,
        &mut compatible,
        &mut findings,
    );
    compare_fingerprinted_revisions(
        "planner support",
        &previous.planner_support,
        &current.planner_support,
        &mut compatible,
        &mut findings,
    );
    let object_was_added = current.types.len() > previous.types.len()
        || current.functions.len() > previous.functions.len()
        || current.operators.len() > previous.operators.len()
        || current.operator_classes.len() > previous.operator_classes.len()
        || current.planner_support.len() > previous.planner_support.len();
    let revision_was_bumped = revisions_were_bumped(&previous, current);
    let declared_type_metadata_changed = previous.types.iter().any(|old| {
        current_types
            .get(old.object_id.as_str())
            .is_some_and(|new| {
                new.name != old.name
                    || new.codec_version != old.codec_version
                    || new.semantic_revision != old.semantic_revision
                    || new.storage_kind != old.storage_kind
                    || new.fixed_bytes != old.fixed_bytes
                    || new.max_bytes != old.max_bytes
                    || new.capabilities != old.capabilities
                    || new.codec_fingerprint != old.codec_fingerprint
            })
    });
    if current.descriptor_fingerprint != previous.descriptor_fingerprint
        && !object_was_added
        && !revision_was_bumped
        && !declared_type_metadata_changed
    {
        compatible = false;
        findings.push(
            "descriptor changed without an added object, codec change, declared type metadata change, or semantic revision bump"
                .to_owned(),
        );
    }
    if findings.is_empty() {
        findings.push("declared identities, codecs, and revisions are monotonic".to_owned());
    }
    Ok(CompatibilityReport {
        format: 1,
        package_id: current.package_id.clone(),
        previous_version: Some(previous.version),
        current_version: current.version.clone(),
        compatible,
        findings,
    })
}

fn compare_revisions(
    kind: &str,
    previous: &[crate::inspect::ObjectReport],
    current: &[crate::inspect::ObjectReport],
    compatible: &mut bool,
    findings: &mut Vec<String>,
) {
    let current = current
        .iter()
        .map(|value| (value.object_id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    for old in previous {
        match current.get(old.object_id.as_str()) {
            None => {
                *compatible = false;
                findings.push(format!("{kind} {} was removed", old.local_id));
            }
            Some(new) if new.semantic_revision < old.semantic_revision => {
                *compatible = false;
                findings.push(format!(
                    "{kind} {} semantic revision regressed",
                    old.local_id
                ));
            }
            Some(_) => {}
        }
    }
}

fn compare_fingerprinted_revisions(
    kind: &str,
    previous: &[crate::inspect::FingerprintedObjectReport],
    current: &[crate::inspect::FingerprintedObjectReport],
    compatible: &mut bool,
    findings: &mut Vec<String>,
) {
    let current = current
        .iter()
        .map(|value| (value.object_id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    for old in previous {
        match current.get(old.object_id.as_str()) {
            None => {
                *compatible = false;
                findings.push(format!("{kind} {} was removed", old.local_id));
            }
            Some(new) if new.semantic_revision < old.semantic_revision => {
                *compatible = false;
                findings.push(format!(
                    "{kind} {} semantic revision regressed",
                    old.local_id
                ));
            }
            Some(new)
                if new.fingerprint != old.fingerprint
                    && new.semantic_revision == old.semantic_revision =>
            {
                *compatible = false;
                findings.push(format!(
                    "{kind} {} changed fingerprint without a semantic revision bump",
                    old.local_id
                ));
            }
            Some(_) => {}
        }
    }
}

fn revisions_were_bumped(previous: &DescriptorReport, current: &DescriptorReport) -> bool {
    let previous_revisions = previous
        .types
        .iter()
        .map(|value| (value.object_id.as_str(), value.semantic_revision))
        .chain(
            previous
                .functions
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            previous
                .operators
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            previous
                .operator_classes
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            previous
                .planner_support
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .collect::<BTreeMap<_, _>>();
    current
        .types
        .iter()
        .map(|value| (value.object_id.as_str(), value.semantic_revision))
        .chain(
            current
                .functions
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            current
                .operators
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            current
                .operator_classes
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .chain(
            current
                .planner_support
                .iter()
                .map(|value| (value.object_id.as_str(), value.semantic_revision)),
        )
        .any(|(id, revision)| {
            previous_revisions
                .get(id)
                .is_some_and(|old| revision > *old)
        })
}

fn validate_golden_metadata(report: &DescriptorReport, golden: &CodecGoldenFile) -> Result<()> {
    if golden.format != 1 || golden.package_id != report.package_id {
        return Err(fail("codec golden root metadata differs from descriptor"));
    }
    if golden.types.len() != report.types.len() {
        return Err(fail("every external type requires codec golden vectors"));
    }
    Ok(())
}

fn verify_provenance(
    directory: &Path,
    manifest: &PluginPackageManifest,
    report: &DescriptorReport,
) -> Result<()> {
    let source = fs::read_to_string(directory.join(PROVENANCE_FILE))
        .map_err(|error| fail(format!("cannot read package provenance: {error}")))?;
    let provenance: BuildProvenance = toml::from_str(&source)
        .map_err(|error| fail(format!("invalid package provenance: {error}")))?;
    let compatibility = read_compatibility(directory)?;
    let expected_tool = format!("cargo-radixdb-plugin {}", env!("CARGO_PKG_VERSION"));
    if provenance.format != 1
        || provenance.tool != expected_tool
        || provenance.rustc != project::OFFICIAL_RUSTC
        || provenance.cargo != project::OFFICIAL_CARGO
        || provenance.build_image != OFFICIAL_BUILD_IMAGE
        || provenance.target != SUPPORTED_TARGET
        || provenance.panic_strategy != "unwind"
        || provenance.manifest_sha256 != hex(&sha256_file(&directory.join(PLUGIN_MANIFEST_FILE))?)
        || provenance.library_sha256 != manifest.library_sha256
        || provenance.descriptor_fingerprint != report.descriptor_fingerprint
        || provenance.codec_golden_sha256 != hex(&sha256_file(&directory.join(GOLDEN_FILE))?)
        || provenance.compatibility_report_sha256
            != hex(&sha256_file(&directory.join(COMPATIBILITY_FILE))?)
        || compatibility.format != 1
        || compatibility.package_id != report.package_id
        || compatibility.current_version != report.version
        || !compatibility.compatible
    {
        return Err(fail("package provenance/checksum parity failed"));
    }
    validate_digest(&provenance.source_manifest_sha256, "source_manifest_sha256")?;
    validate_digest(&provenance.source_lock_sha256, "source_lock_sha256")?;
    Ok(())
}

fn read_compatibility(directory: &Path) -> Result<CompatibilityReport> {
    let source = fs::read_to_string(directory.join(COMPATIBILITY_FILE))
        .map_err(|error| fail(format!("cannot read compatibility report: {error}")))?;
    let report: CompatibilityReport = toml::from_str(&source)
        .map_err(|error| fail(format!("invalid compatibility report: {error}")))?;
    if report.findings.is_empty() {
        return Err(fail("compatibility report must contain evidence findings"));
    }
    Ok(report)
}

fn validate_digest(value: &str, name: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(fail(format!(
            "{name} must be a lowercase SHA-256 hexadecimal digest"
        )));
    }
    Ok(())
}

fn read_manifest(directory: &Path) -> Result<PluginPackageManifest> {
    let source = fs::read_to_string(directory.join(PLUGIN_MANIFEST_FILE))
        .map_err(|error| fail(format!("cannot read package manifest: {error}")))?;
    let manifest: PluginPackageManifest = toml::from_str(&source)
        .map_err(|error| fail(format!("invalid package manifest: {error}")))?;
    manifest
        .validate_static_fields()
        .map_err(|error| fail(format!("invalid package manifest: {error}")))?;
    Ok(manifest)
}

pub(crate) fn require_official_environment() -> Result<()> {
    match std::env::var("RADIXDB_PLUGIN_BUILD_IMAGE") {
        Ok(value) if value == OFFICIAL_BUILD_IMAGE => {}
        _ => Err(fail(format!(
            "release packaging requires RADIXDB_PLUGIN_BUILD_IMAGE={OFFICIAL_BUILD_IMAGE} inside the official build environment"
        )))?,
    }
    let os_release = fs::read_to_string("/etc/os-release").map_err(|error| {
        fail(format!(
            "cannot identify official build environment: {error}"
        ))
    })?;
    let is_bookworm = os_release.lines().any(|line| line == "ID=debian")
        && os_release
            .lines()
            .any(|line| line == "VERSION_CODENAME=bookworm");
    if !is_bookworm {
        return Err(fail(
            "release packaging is only allowed inside Debian bookworm official build image",
        ));
    }
    project::validate_toolchain(SUPPORTED_TARGET)?;
    project::validate_official_tool_versions()
}

fn admit_isolated(executable: &Path, directory: &Path) -> Result<()> {
    let output = isolated_command(executable)
        .arg("__admission-child")
        .arg("--package")
        .arg(directory)
        .output()
        .map_err(|error| fail(format!("cannot start isolated package admission: {error}")))?;
    if !output.status.success() {
        return Err(fail(format!(
            "isolated PLUG-20 admission failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn absolute_new_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Err(fail(format!(
            "package output already exists; refusing overwrite: {}",
            path.display()
        )));
    }
    let file = path
        .file_name()
        .ok_or_else(|| fail("package output must name a directory"))?;
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| {
        fail(format!(
            "cannot canonicalize package output parent: {error}"
        ))
    })?;
    Ok(parent.join(file))
}

fn write_toml<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let source = toml::to_string(value)
        .map_err(|error| fail(format!("cannot encode {}: {error}", path.display())))?;
    fs::write(path, source)
        .map_err(|error| fail(format!("cannot write {}: {error}", path.display())))?;
    set_mode(path, 0o644)
}

fn library_component(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn validate_glibc_ceiling(actual: &str) -> Result<()> {
    let parse = |value: &str| -> Option<(u32, u32)> {
        let (major, minor) = value.split_once('.')?;
        Some((major.parse().ok()?, minor.parse().ok()?))
    };
    let actual = parse(actual).ok_or_else(|| fail("invalid detected GLIBC version"))?;
    let ceiling = parse(MAXIMUM_REQUIRED_GLIBC).expect("constant is valid");
    if actual > ceiling {
        return Err(fail(format!(
            "plugin requires GLIBC {}.{}, above supported {MAXIMUM_REQUIRED_GLIBC}",
            actual.0, actual.1
        )));
    }
    Ok(())
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|error| {
        fail(format!(
            "cannot set permissions on {}: {error}",
            path.display()
        ))
    })
}

fn sync_file(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| fail(format!("cannot sync {}: {error}", path.display())))
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| fail(format!("cannot sync directory {}: {error}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn package_component_is_stable_and_path_safe() {
        assert_eq!(library_component("radix.spatial-v1"), "radix_spatial_v1");
    }
}
