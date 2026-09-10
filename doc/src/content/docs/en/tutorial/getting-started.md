---
title: Getting Started
description: Build and start the local RadixDB command-line client.
---

The tutorial assumes a Linux x86-64 build host, a Unix shell and
a source checkout of RadixDB. A database server is not required. Other systems
need their own build and filesystem checks before these instructions can be
treated as verified there.

## Build the Client

Install the Rust toolchain specified by `rust-toolchain.toml`, including Cargo,
and the system C compiler and linker. The checked source uses Rust 1.97.0.
Cargo needs access to its package registry, or an already populated dependency
cache. Run the following from the repository root:

```sh
cargo build --locked --bin radixdb-cli --features cli
./target/debug/radixdb-cli --version
export PATH="$PWD/target/debug:$PATH"
```

`--locked` prevents the build from updating dependency resolution.
The version output includes the application version, Git revision and lockfile
digest. Record these when reporting a problem. This manual targets RadixDB 1.2
development; the footer reports the documentation target and the application
version separately. Include the Git revision when comparing a reported result
with this manual.

This is an unoptimized debug build suitable for following the tutorial.
It is not a performance baseline. The later installation chapter covers
release packaging and service deployment.

## Check the Client

```sh
radixdb-cli -d memory:// -e "SELECT 1"
```

The result contains one row with the value 1. This process-local database
is discarded when the client exits. The next chapter uses a file database
so that separate sessions share the same data.

Continue with [your first database](../first-database/). Keep the same shell
session so that the updated `PATH` remains available.
