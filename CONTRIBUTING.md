# Contributing to RadixDB

[Русский](CONTRIBUTING.ru.md)

RadixDB 1.1.0 is the latest accepted release. Changes after its annotated tag
are unreleased until a later release records them. Contributions should
preserve correctness, durability and documented contracts. See
[CHANGELOG.md](CHANGELOG.md) for release history.

## Before changing code

- Read [README.md](README.md) and the current limitations.
- Use the [documentation workspace](doc/README.md) for the manual structure,
  validation commands and public evidence archive.
- Record parser/executor/documentation mismatches in an issue with a
  reproduction and acceptance criteria.
- Keep public SQL claims backed by code, tests or runnable examples.

## Development commands

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --features cli,bench-harness
cargo check --locked --workspace --tests --bins --features cli,bench-harness
cargo test --locked -p radixdb-client
```

Useful focused tests:

```bash
cargo test --test data_type_contract_test -- --nocapture
cargo test --test partial_index_tcp_client_test -- --test-threads=1 --nocapture
cargo test --test pragma_test -- --nocapture
```

Heavy benchmarks and destructive/recovery tests should be run only on an
explicit test data directory, not on a directory containing important data.

## Commit style

- Keep commits focused.
- Separate docs-only changes from code changes when practical.
- Do not commit generated databases, WAL files, benchmark CSVs, logs, release
  binaries or local configuration with secrets.

## Licensing

Contributors retain copyright in their work. Before a Contribution is accepted,
the contributor must record acceptance of [CLA.md](CLA.md). The CLA grants the
licensing steward the rights needed to distribute each component under the
license assigned in [LICENSING.md](LICENSING.md) and to offer separate
commercial licenses.
