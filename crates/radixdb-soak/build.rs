use std::{path::PathBuf, process::Command};

use sha2::{Digest, Sha256};

fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest.join("../..");
    let revision = std::env::var("RADIXDB_GIT_COMMIT")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| git_revision(&root));
    assert_revision(&revision);

    let lock = std::fs::read(root.join("Cargo.lock")).expect("read workspace Cargo.lock");
    let lock_sha256 = format!("{:x}", Sha256::digest(lock));
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".into());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".into());

    println!("cargo:rustc-env=RADIXDB_SOAK_GIT_COMMIT={revision}");
    println!("cargo:rustc-env=RADIXDB_SOAK_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=RADIXDB_SOAK_BUILD_TARGET={target}");
    println!("cargo:rustc-env=RADIXDB_SOAK_CARGO_LOCK_SHA256={lock_sha256}");
    println!("cargo:rerun-if-env-changed=RADIXDB_GIT_COMMIT");
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.lock").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/index").display()
    );
}

fn git_revision(root: &std::path::Path) -> String {
    let output = Command::new("git")
        .args([
            "-C",
            root.to_str().unwrap(),
            "rev-parse",
            "--verify",
            "HEAD",
        ])
        .output()
        .expect("run git rev-parse");
    assert!(output.status.success(), "git rev-parse failed");
    let mut revision = String::from_utf8(output.stdout)
        .expect("git revision is UTF-8")
        .trim()
        .to_ascii_lowercase();
    let status = Command::new("git")
        .args([
            "-C",
            root.to_str().unwrap(),
            "status",
            "--porcelain",
            "--untracked-files=no",
        ])
        .output()
        .expect("run git status");
    assert!(status.status.success(), "git status failed");
    if !status.stdout.is_empty() {
        revision.push_str("-dirty");
    }
    revision
}

fn assert_revision(revision: &str) {
    let (hash, dirty) = revision
        .strip_suffix("-dirty")
        .map_or((revision, false), |hash| (hash, true));
    assert!(
        hash.len() == 40 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "RADIXDB_GIT_COMMIT must be a full hexadecimal revision"
    );
    if dirty {
        assert!(revision.ends_with("-dirty"));
    }
}
