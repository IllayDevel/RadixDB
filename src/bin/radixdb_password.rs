use std::io::{IsTerminal, Read, Write};
use std::process::ExitCode;

const USAGE: &str = "usage: radixdb-password [--help | --version]\n\
Reads one password line from standard input and writes an Argon2id PHC verifier.\n\
The password is never accepted as a command-line argument, and terminal input is refused.";

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    if args.as_slice() == ["--version"] {
        println!("{}", radixdb::server::version_line("radixdb-password"));
        return ExitCode::SUCCESS;
    }
    if !args.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }

    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        eprintln!(
            "radixdb-password: refusing echoed terminal input; pipe a password from a hidden-input reader"
        );
        return ExitCode::FAILURE;
    }

    match generate_from(stdin.lock(), std::io::stdout().lock()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("radixdb-password: {error}");
            ExitCode::FAILURE
        }
    }
}

fn generate_from(mut input: impl Read, mut output: impl Write) -> Result<(), String> {
    let limit = radixdb::executor::credentials::MAX_PASSWORD_BYTES + 3;
    let mut bytes = Vec::with_capacity(limit);
    input
        .by_ref()
        .take(limit as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read password from standard input: {error}"))?;
    if bytes.len() == limit {
        return Err("password input exceeds 1024 bytes".to_string());
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.contains(&b'\n') || bytes.contains(&b'\r') {
        return Err("standard input must contain exactly one password line".to_string());
    }
    let password =
        String::from_utf8(bytes).map_err(|_| "password must be valid UTF-8".to_string())?;
    let encoded = radixdb::executor::credentials::hash_password_verifier(&password)
        .map_err(|error| error.to_string())?;
    writeln!(output, "{encoded}").map_err(|error| format!("failed to write verifier: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdin_password_round_trips_through_generated_verifier() {
        let mut output = Vec::new();
        generate_from(&b"root secret\n"[..], &mut output).unwrap();
        let encoded = String::from_utf8(output).unwrap();
        assert!(encoded.starts_with("$argon2id$"));
        assert!(radixdb::executor::credentials::verify_password_verifier(
            encoded.trim_end(),
            "root secret"
        ));
    }

    #[test]
    fn empty_multiline_and_oversized_input_are_rejected() {
        assert!(generate_from(&b"\n"[..], Vec::new()).is_err());
        assert!(generate_from(&b"one\ntwo\n"[..], Vec::new()).is_err());
        let oversized = vec![b'x'; radixdb::executor::credentials::MAX_PASSWORD_BYTES + 1];
        assert!(generate_from(&oversized[..], Vec::new()).is_err());
    }
}
