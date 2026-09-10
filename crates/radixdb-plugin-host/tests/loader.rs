use std::{
    fs,
    os::unix::fs::{symlink, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    thread,
};

use radixdb_plugin_host::{
    load_plugin_registry, DatabasePluginAdmission, PackageRequirement, PluginHostConfig,
    RequirementIssue,
};
use semver::Version;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const PACKAGE_ID: [u8; 16] = [
    0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
];
const FINGERPRINT: [u8; 32] = [0x11; 32];

fn write_package(root: &Path, directory_name: &str, version: &str) -> PathBuf {
    let directory = root.join(directory_name);
    let library_directory = directory.join("lib");
    fs::create_dir_all(&library_directory).unwrap();
    let source = directory.join("plugin.c");
    fs::write(
        &source,
        include_str!("fixtures/minimal_plugin.c").replace("@VERSION@", version),
    )
    .unwrap();
    let library = library_directory.join("libsample.so");
    let include = Path::new(env!("CARGO_MANIFEST_DIR")).join("../radixdb-plugin-abi/include");
    let output = Command::new("cc")
        .args(["-std=c11", "-shared", "-fPIC", "-fvisibility=hidden"])
        .arg(format!("-I{}", include.display()))
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .output()
        .expect("C compiler must be available for the independent ABI fixture");
    assert!(
        output.status.success(),
        "fixture compile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let digest: [u8; 32] = Sha256::digest(fs::read(&library).unwrap()).into();
    let hash = hex(digest);
    let manifest = format!(
        r#"format = 1
package_id = "12345678-1234-1234-1234-123456789abc"
name = "sample"
version = "{version}"
library = "lib/libsample.so"
library_sha256 = "{hash}"
descriptor_fingerprint = "{}"
target = "x86_64-unknown-linux-gnu"
maximum_required_glibc = "2.36"
build_image = "rust:1.97.0-bookworm"
panic_strategy = "unwind"
abi_major = 1
abi_min_minor = 0
abi_max_minor = 0
"#,
        hex(FINGERPRINT)
    );
    fs::write(directory.join("radixdb-plugin.toml"), manifest).unwrap();
    directory
}

fn hex<const N: usize>(bytes: [u8; N]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn config(paths: impl IntoIterator<Item = PathBuf>) -> PluginHostConfig {
    PluginHostConfig {
        package_directories: paths.into_iter().collect(),
    }
}

#[test]
fn empty_configuration_does_not_discover_cwd_or_create_a_generation() {
    let registry = load_plugin_registry(&PluginHostConfig::default()).unwrap();
    assert_eq!(registry.status().generation, 0);
    assert_eq!(registry.status().packages, 0);
}

#[test]
fn c_package_loads_and_registry_is_safe_for_concurrent_readers() {
    let temporary = TempDir::new().unwrap();
    let package = write_package(temporary.path(), "sample-v1", "1.0.0");
    let registry = load_plugin_registry(&config([package])).unwrap();
    assert!(registry.generation() > 0);
    assert_eq!(registry.status().packages, 1);
    assert_eq!(
        registry.package(&PACKAGE_ID).unwrap().version,
        Version::new(1, 0, 0)
    );

    let workers: Vec<_> = (0..16)
        .map(|_| {
            let registry = Arc::clone(&registry);
            thread::spawn(move || {
                for _ in 0..1_000 {
                    assert_eq!(registry.status().packages, 1);
                    assert_eq!(
                        registry.package(&PACKAGE_ID).unwrap().package_id,
                        PACKAGE_ID
                    );
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn highest_semver_is_active_and_older_version_is_observable_as_shadowed() {
    let temporary = TempDir::new().unwrap();
    let old = write_package(temporary.path(), "sample-v1", "1.0.0");
    let new = write_package(temporary.path(), "sample-v2", "2.0.0");
    let registry = load_plugin_registry(&config([old, new])).unwrap();
    assert_eq!(
        registry.package(&PACKAGE_ID).unwrap().version,
        Version::new(2, 0, 0)
    );
    assert_eq!(registry.status().shadowed_versions, 1);
}

#[test]
fn duplicate_identity_version_fails_before_publication() {
    let temporary = TempDir::new().unwrap();
    let first = write_package(temporary.path(), "sample-a", "1.0.0");
    let second = write_package(temporary.path(), "sample-b", "1.0.0");
    let error = load_plugin_registry(&config([first, second])).unwrap_err();
    assert!(error.to_string().contains("duplicate package identity"));
}

#[test]
fn checksum_failure_rejects_whole_configured_set() {
    let temporary = TempDir::new().unwrap();
    let valid = write_package(temporary.path(), "sample-good", "2.0.0");
    let broken = write_package(temporary.path(), "sample-bad", "1.0.0");
    let library = broken.join("lib/libsample.so");
    let mut bytes = fs::read(&library).unwrap();
    bytes.push(0xaa);
    fs::write(&library, bytes).unwrap();
    let error = load_plugin_registry(&config([valid, broken])).unwrap_err();
    assert!(error.to_string().contains("SHA-256 mismatch"));
}

#[test]
fn relative_symlink_and_writable_artifacts_are_rejected() {
    let temporary = TempDir::new().unwrap();
    let package = write_package(temporary.path(), "sample-v1", "1.0.0");

    let relative = load_plugin_registry(&config([PathBuf::from("sample-v1")])).unwrap_err();
    assert!(relative.to_string().contains("absolute"));

    let link = temporary.path().join("package-link");
    symlink(&package, &link).unwrap();
    let linked = load_plugin_registry(&config([link])).unwrap_err();
    assert!(linked.to_string().contains("symbolic link"));

    let manifest = package.join("radixdb-plugin.toml");
    let mut permissions = fs::metadata(&manifest).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&manifest, permissions).unwrap();
    let writable = load_plugin_registry(&config([package])).unwrap_err();
    assert!(writable.to_string().contains("permissions"));

    let package = write_package(temporary.path(), "writable-library", "1.0.1");
    let library = package.join("lib/libsample.so");
    let mut permissions = fs::metadata(&library).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&library, permissions).unwrap();
    let writable = load_plugin_registry(&config([package])).unwrap_err();
    assert!(writable.to_string().contains("permissions"));

    let package = write_package(temporary.path(), "writable-directory", "1.0.2");
    let mut permissions = fs::metadata(&package).unwrap().permissions();
    permissions.set_mode(0o777);
    fs::set_permissions(&package, permissions).unwrap();
    let writable = load_plugin_registry(&config([package])).unwrap_err();
    assert!(writable.to_string().contains("permissions"));
}

#[test]
fn requirements_restrict_only_the_database_being_assessed() {
    let temporary = TempDir::new().unwrap();
    let package = write_package(temporary.path(), "sample-v1", "1.0.0");
    let registry = load_plugin_registry(&config([package])).unwrap();
    let good = PackageRequirement {
        package_id: PACKAGE_ID,
        version: Version::new(1, 0, 0),
        abi_major: radixdb_plugin_abi::RADIX_ABI_MAJOR,
        abi_min_minor: radixdb_plugin_abi::RADIX_ABI_MINOR,
        abi_max_minor: radixdb_plugin_abi::RADIX_ABI_MINOR,
        descriptor_fingerprint: FINGERPRINT,
        objects: Vec::new(),
    };
    assert_eq!(
        registry.assess_requirements(std::slice::from_ref(&good)),
        DatabasePluginAdmission::Normal
    );
    let mut stale = good.clone();
    stale.version = Version::new(2, 0, 0);
    assert!(matches!(
        registry.assess_requirements(&[stale]),
        DatabasePluginAdmission::Restricted { issues }
            if matches!(issues.as_slice(), [RequirementIssue::PackageVersion { .. }])
    ));
    let mut stale = good.clone();
    stale.abi_max_minor = stale.abi_max_minor.saturating_add(1);
    assert!(matches!(
        registry.assess_requirements(&[stale]),
        DatabasePluginAdmission::Restricted { issues }
            if matches!(issues.as_slice(), [RequirementIssue::PackageAbi { .. }])
    ));
    let mut stale = good.clone();
    stale.descriptor_fingerprint[0] ^= 1;
    assert!(matches!(
        registry.assess_requirements(&[stale]),
        DatabasePluginAdmission::Restricted { issues }
            if matches!(issues.as_slice(), [RequirementIssue::DescriptorFingerprint { .. }])
    ));
    let mut missing = good;
    missing.package_id[0] ^= 1;
    assert!(matches!(
        registry.assess_requirements(&[missing]),
        DatabasePluginAdmission::Restricted { issues }
            if matches!(issues.as_slice(), [RequirementIssue::MissingPackage { .. }])
    ));
    assert_eq!(
        registry.assess_requirements(&[]),
        DatabasePluginAdmission::Normal,
        "another database remains independent"
    );
}
