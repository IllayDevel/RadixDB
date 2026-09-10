// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

fn is_full_git_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn resolve_git_revision() -> String {
    if let Ok(value) = std::env::var("RADIXDB_GIT_COMMIT") {
        let value = value.trim().to_ascii_lowercase();
        if !is_full_git_revision(&value) {
            panic!("RADIXDB_GIT_COMMIT must be a full 40-character hexadecimal revision");
        }
        return value;
    }

    let revision = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| is_full_git_revision(value));

    let Some(mut revision) = revision else {
        return format!("source-{}", env!("CARGO_PKG_VERSION"));
    };
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| !output.stdout.is_empty());
    if dirty {
        revision.push_str("-dirty");
    }
    revision
}

/// Generate a deterministic UTC timestamp without external dependencies.
fn chrono_free_timestamp(secs: u64) -> String {
    // Convert to UTC date-time components
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Civil date from days since epoch (algorithm from Howard Hinnant)
    let z = days as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, minutes, seconds
    )
}

fn main() {
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let git_revision = resolve_git_revision();
    println!("cargo:rustc-env=RADIXDB_GIT_COMMIT={git_revision}");
    println!("cargo:rustc-env=RADIXDB_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=RADIXDB_BUILD_TARGET={target}");
    let cargo_lock = std::fs::read("Cargo.lock").expect("read Cargo.lock for build identity");
    let cargo_lock_sha256 = format!("{:x}", Sha256::digest(cargo_lock));
    println!("cargo:rustc-env=RADIXDB_CARGO_LOCK_SHA256={cargo_lock_sha256}");

    let build_time = std::env::var("RADIXDB_BUILD_TIME").unwrap_or_else(|_| {
        let epoch = std::env::var("SOURCE_DATE_EPOCH")
            .ok()
            .map(|value| {
                value
                    .parse::<u64>()
                    .expect("SOURCE_DATE_EPOCH must be an unsigned Unix timestamp")
            })
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("build clock must not precede the Unix epoch")
                    .as_secs()
            });
        chrono_free_timestamp(epoch)
    });
    println!("cargo:rustc-env=RADIXDB_BUILD_TIME={build_time}");
    println!("cargo:rerun-if-env-changed=RADIXDB_BUILD_TIME");

    // Only re-run if HEAD changes or env var is set
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-changed=.git/refs/heads/");
    println!("cargo:rerun-if-env-changed=RADIXDB_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-changed=Cargo.lock");
}
