// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

fn main() -> std::process::ExitCode {
    let mut arguments = std::env::args().skip(1);
    let Some(argument) = arguments.next() else {
        eprintln!("radixdb-smoke-client: usage: radixdb-smoke-client HOST:PORT | --version");
        return std::process::ExitCode::FAILURE;
    };
    if arguments.next().is_some() {
        eprintln!("radixdb-smoke-client: usage: radixdb-smoke-client HOST:PORT | --version");
        return std::process::ExitCode::FAILURE;
    }
    if argument == "--version" {
        println!("{}", radixdb::server::version_line("radixdb-smoke-client"));
        return std::process::ExitCode::SUCCESS;
    }

    match radixdb::server::probe_smoke_endpoint(&argument) {
        Ok(report) => {
            println!("{report}");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("radixdb-smoke-client: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
