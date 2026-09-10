use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn generated_root_verifier_matches_the_stdin_password() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_radixdb-password"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start radixdb-password");
    child
        .stdin
        .take()
        .expect("password stdin")
        .write_all(b"root integration secret\n")
        .expect("write password");
    let output = child.wait_with_output().expect("wait for generator");
    assert!(output.status.success(), "{:?}", output.stderr);
    let encoded = String::from_utf8(output.stdout).expect("UTF-8 verifier");
    assert!(radixdb::executor::credentials::verify_password_verifier(
        encoded.trim_end(),
        "root integration secret"
    ));
}

#[test]
fn command_line_password_is_refused_without_echoing_it() {
    let secret = "do-not-echo-this-secret";
    let output = Command::new(env!("CARGO_BIN_EXE_radixdb-password"))
        .arg(secret)
        .output()
        .expect("run radixdb-password");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
}
