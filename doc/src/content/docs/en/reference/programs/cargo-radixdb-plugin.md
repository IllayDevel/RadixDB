---
title: cargo radixdb-plugin
description: Validate, test, inspect, build and package native RadixDB extensions.
---

`cargo radixdb-plugin` is the supported command-line tool for a Rust native
extension. It enforces the public SDK boundary, deterministic build profile,
descriptor contract and package admission rules before an artifact reaches a
server.

## Invocation

Install or build the `cargo-radixdb-plugin` binary, then use either equivalent
form:

```sh
cargo radixdb-plugin check --manifest-path Cargo.toml
cargo-radixdb-plugin check --manifest-path Cargo.toml
```

## Commands

```text
cargo radixdb-plugin check
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin build
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin test-host
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin inspect
    (--library PATH | --package PATH)

cargo radixdb-plugin package
    [--manifest-path PATH]
    [--target-dir PATH]
    --output-dir PATH
    [--golden PATH]
    [--previous-package PATH]
```

`--manifest-path` defaults to `Cargo.toml`. The default target directory is the
project Cargo target directory followed by `radixdb-plugin`.

### check

Validates the project shape and runs a locked release check for
`x86_64-unknown-linux-gnu`. The project must contain exactly one `cdylib`, a
`Cargo.lock`, `panic = "unwind"` for release builds and a normal dependency on
the public `radixdb-plugin` crate. Other `radixdb-*` crates can appear only as
development dependencies.

```sh
cargo radixdb-plugin check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

### build

Builds the locked release library with the supported toolchain and deterministic
link settings, then inspects the exported descriptor in an isolated child
process. The command prints the `.so` path to standard output and descriptor
identity to standard error.

`build` is useful during development. A server installation should use the
complete directory produced by `package`, not a bare library.

### test-host

Runs the extension's release tests, builds and inspects the library, then checks
every external type against `radixdb-plugin-golden.toml` in an isolated child
process.

```sh
cargo radixdb-plugin test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

The golden file must list every exported external type exactly once. Each
vector is canonical lowercase hexadecimal and must decode and re-encode to the
same bytes.

### inspect

`--library` loads one raw `.so` in an isolated process and prints the normalized
descriptor as JSON. `--package` verifies the complete package layout, manifest,
checksums, provenance, compatibility report and codec vectors before printing
the same report.

```sh
cargo radixdb-plugin inspect --library target/plugin/lib/libexample.so
cargo radixdb-plugin inspect --package dist/example-1.0.0
```

The two input options are mutually exclusive and one is required.

### package

Runs tests and a deterministic build, validates descriptor and codec evidence,
and creates a complete host-admissible package atomically at `--output-dir`.
That final package directory must not already exist.

```sh
mkdir -p dist
RADIXDB_PLUGIN_BUILD_IMAGE=rust:1.97.0-bookworm \
cargo radixdb-plugin package \
  --manifest-path Cargo.toml \
  --output-dir dist/radixdb-pair-1.0.0
```

Release packaging is accepted only inside the official Debian bookworm build
image with exactly Rust and Cargo 1.97.0. Setting the environment variable on a
different host does not bypass the OS and toolchain checks.

`--golden` selects a non-default codec vector file. `--previous-package`
compares stable object identities, codecs and semantic revisions with an
existing complete package. An incompatible comparison fails packaging. This
report is evidence; it does not migrate a database binding.

## Package result

The output directory is the immutable package directory:

```text
radixdb-pair-1.0.0/
  radixdb-plugin.toml
  radixdb-plugin-golden.toml
  radixdb-plugin-provenance.toml
  radixdb-plugin-compatibility.toml
  lib/
    libradixdb_pair.so
```

The manifest records package identity, ABI range, descriptor fingerprint,
target, glibc ceiling and library checksum. Provenance binds the library,
golden vectors and compatibility report to the official build environment.
The tool stages files, synchronizes them and renames the directory only after
local admission succeeds.

## Required toolchain

Development checks require Rust 1.97.0 with host target
`x86_64-unknown-linux-gnu`. The initial ABI supports ELF64 little-endian GNU
Linux and glibc no newer than 2.36. Cross-target artifacts are rejected.

## Exit status

Zero means the requested validation or build completed. Nonzero reports a
project, compiler, descriptor, codec, compatibility, package-layout or
admission failure. Treat any nonzero result as a release blocker; do not copy a
partially staged directory into the server allowlist.

## See also

See [Developing native extensions](../../../programming/native-extensions/),
[Installing and operating extensions](../../../administration/extensions/) and
[Native extension SQL](../../sql/extensions/).
