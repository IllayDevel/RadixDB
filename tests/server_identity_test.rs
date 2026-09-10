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

use radixdb_client::PROTOCOL_VERSION;

#[test]
fn server_version_is_complete_and_does_not_start_a_listener() {
    let output = Command::new(env!("CARGO_BIN_EXE_radixdb-server"))
        .arg("--version")
        .output()
        .expect("run radixdb-server --version");

    assert!(
        output.status.success(),
        "--version failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());

    let line = String::from_utf8(output.stdout).expect("version output must be UTF-8");
    let line = line.trim();
    assert!(line.starts_with(&format!("radixdb-server {} ", env!("CARGO_PKG_VERSION"))));
    assert!(line.contains(&format!("git={}", env!("RADIXDB_GIT_COMMIT"))));
    assert!(line.contains(&format!("protocol={PROTOCOL_VERSION}")));
    assert!(line.contains(&format!("profile={}", env!("RADIXDB_BUILD_PROFILE"))));
    assert!(line.contains(&format!("target={}", env!("RADIXDB_BUILD_TARGET"))));

    let revision = env!("RADIXDB_GIT_COMMIT");
    let exact_revision = revision.strip_suffix("-dirty").unwrap_or(revision);
    assert!(
        revision.starts_with("source-")
            || (exact_revision.len() == 40
                && exact_revision.bytes().all(|byte| byte.is_ascii_hexdigit())),
        "unverifiable source identity: {revision}"
    );
    assert_ne!(env!("RADIXDB_BUILD_TIME"), "unknown");
}

#[test]
fn server_rejects_ambiguous_version_invocation() {
    let output = Command::new(env!("CARGO_BIN_EXE_radixdb-server"))
        .args(["--version", "--config", "server.toml"])
        .output()
        .expect("run invalid radixdb-server invocation");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
}
