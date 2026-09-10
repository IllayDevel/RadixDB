use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn is_full_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn git_revision() -> String {
    let Some(mut revision) = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| is_full_revision(value))
    else {
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

fn utc_timestamp(seconds: u64) -> String {
    let days = seconds / 86_400;
    let time = seconds % 86_400;
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let day_of_year = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time / 3_600,
        (time % 3_600) / 60,
        time % 60
    )
}

fn main() {
    let revision = std::env::var("RADIXDB_GIT_COMMIT")
        .map(|value| value.trim().to_ascii_lowercase())
        .inspect(|value| assert!(is_full_revision(value), "invalid RADIXDB_GIT_COMMIT"))
        .unwrap_or_else(|_| git_revision());
    let build_time = std::env::var("RADIXDB_BUILD_TIME").unwrap_or_else(|_| {
        let seconds = std::env::var("SOURCE_DATE_EPOCH")
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
        utc_timestamp(seconds)
    });
    println!("cargo:rustc-env=RADIXDB_GIT_COMMIT={revision}");
    println!("cargo:rustc-env=RADIXDB_BUILD_TIME={build_time}");
    println!("cargo:rerun-if-env-changed=RADIXDB_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=RADIXDB_BUILD_TIME");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
}
