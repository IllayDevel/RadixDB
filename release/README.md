# RadixDB test release layout

This directory is a local test runtime for connecting external clients to the
RadixDB TCP server. It is a repository sandbox, not the canonical Linux service
installation.

Two layouts are documented:

| Layout | Purpose | Data path |
| --- | --- | --- |
| `release/` | Local smoke/runtime sandbox inside the source checkout. | `release/data/` |
| `/opt/radixdb/` | Canonical systemd test-stand installation. | `/opt/radixdb/data` |

Use the sandbox for local smoke tests and client development. Use the `/opt`
layout when testing service ownership, systemd restart and installed binaries.

## Build

From repository root:

```bash
cargo build --locked --release \
  --bin radixdb-server --bin radixdb-password --bin radixdb-cli \
  --bin radixdb-smoke-client \
  --features cli
install -Dm755 target/release/radixdb-server release/bin/radixdb-server
install -Dm755 target/release/radixdb-password release/bin/radixdb-password
install -Dm755 target/release/radixdb-cli release/bin/radixdb-cli
install -Dm755 target/release/radixdb-smoke-client release/bin/radixdb-smoke-client
```

The binaries in `release/bin/` are local artifacts and are intentionally ignored
by Git.

Canonical release bundles are produced by `package-artifacts.sh`. It removes
only embedded DWARF sections from installed executables and writes them to
`debug/<binary>.debug`; every installed binary retains a `.gnu_debuglink` and
the same ELF Build ID as its diagnostic artifact. This keeps the deployed
binary compact without discarding post-mortem symbols. The unstripped
`target/release` files remain local developer artifacts.

## Run

```bash
cd "$RADIXDB_REPO/release"
./start.sh
./status.sh
./smoke-client.sh
./stop.sh
```

If the default port is already used by a system service, run the sandbox with a
temporary config:

```bash
RADIXDB_RELEASE_CONFIG=/path/to/server-smoke.toml ./start.sh
RADIXDB_RELEASE_CONFIG=/path/to/server-smoke.toml ./smoke-client.sh
./stop.sh
```

Default endpoint:

```text
127.0.0.1:15441
```

Database selection contract:

- the current external protocol has no separate client-facing
  `CREATE DATABASE`;
- `select_database(name)` opens or creates
  `data/databases/<name>` on the server and makes it the active database for
  the connection.

Runtime files:

- `server.toml` — checked-in test config;
- `data/` — local database files, ignored by Git;
- `logs/server.log` — server stdout/stderr, ignored by Git;
- `run/radixdb-server.pid` — identity record (`pid start_time`), ignored by Git.

`release/.gitignore` intentionally excludes:

```text
/bin/
/data/
/logs/
/run/
```

Do not commit generated binaries, database files, WAL, logs or pid files from
this directory.

## Scripts

| Script | Purpose |
| --- | --- |
| `start.sh` | Starts `bin/radixdb-server --config server.toml`, writes logs and pid file, and waits for listener readiness. |
| `bin/radixdb-password` | Reads one password line from redirected stdin and prints an Argon2id PHC verifier for `server.authentication`. |
| `status.sh` | Verifies pid/start-time/executable/listener identity. Exit code `0` means ready, `2` means starting, `3` means stopped, `1` means stale/foreign. |
| `smoke-client.sh` | Runs the packaged smoke-client binary against the configured endpoint. |
| `stop.sh` | Sends `SIGTERM` and waits up to 10 seconds for clean shutdown. |
| `install-systemd.sh` | Installs the tracked hardened unit and binaries; preserves an existing config unless replacement is requested. |
| `uninstall-systemd.sh` | Removes the service and binaries while preserving config/data unless explicit purge is requested. |
| `backup-external.sh` | Creates and checksums a read-only snapshot artifact outside the live database root. |
| `restore-external.sh` | Verifies an external artifact and restores it only into a new database root. |

Important limitation: `start.sh` checks TCP listener readiness. A specific
database may still need to open/recover on first `select_database(name)`. Read
the server log for `state=opening` / `state=ready`.

Full server runtime guide:
[`../doc/src/content/docs/en/administration/server.md`](../doc/src/content/docs/en/administration/server.md).

Русская глава по runtime сервера:
[`../doc/src/content/docs/ru/administration/server.md`](../doc/src/content/docs/ru/administration/server.md).

Full `server.toml` key reference:
[`../doc/src/content/docs/en/reference/configuration/index.md`](../doc/src/content/docs/en/reference/configuration/index.md).

SQL type reference, including `UUID PRIMARY KEY AUTO_INCREMENT`:
[`../doc/src/content/docs/en/sql/types.md`](../doc/src/content/docs/en/sql/types.md).

## GitHub release checklist

Before attaching release artifacts:

1. run `cargo fmt --check`;
2. run the selected test suite for the release gate;
3. build release binaries;
4. install binaries into `release/bin/`;
5. run `bash -n release/*.sh scripts/check-release-lifecycle.sh scripts/check-release-bundle.sh`;
6. run `RADIXDB_TEST_SERVER_BIN=target/release/radixdb-server scripts/check-release-lifecycle.sh`;
7. verify `release/server.toml` still contains only portable defaults;
8. verify no `release/bin/`, `release/data/`, `release/logs/` or
   `release/run/` artifacts are staged;
9. create the verifiable bundle:

   ```bash
   release/package-artifacts.sh dist \
     target/release/radixdb-server target/release/radixdb-cli \
     target/release/radixdb-smoke-client
   ```

10. verify `dist/SHA256SUMS` and retain `dist/PROVENANCE.env` with the release.

The complete automated bundle gate also exercises a dry systemd install and an
immutable external backup restored into a new root:

```bash
scripts/check-release-bundle.sh dist \
  target/release/radixdb-server target/release/radixdb-cli \
  target/release/radixdb-smoke-client
```

`dist` must not exist before either command. Every bundle file, including
license, notice, lifecycle tools and provenance, is covered by `SHA256SUMS`.

## Client crate

External Rust projects should depend on:

```toml
[dependencies]
radixdb-client = { path = "../crates/radixdb-client" }
```

For a separate application repository, replace the path with the checked-out
RadixDB client crate location, or with the published crate once RadixDB starts
publishing crates.
