---
title: radixdb-server
description: Command-line reference for the RadixDB protocol server.
---

`radixdb-server` owns file databases under one data root and listens for
protocol 17 clients. It has a deliberately small command surface.

## Synopsis

```text
radixdb-server [--config server.toml] [--print-endpoint] | --help | --version
```

| Invocation | Result |
| --- | --- |
| `radixdb-server` | Read `server.toml` from the process working directory and start |
| `radixdb-server --config PATH` | Read exactly `PATH` and start |
| `radixdb-server --print-endpoint` | Parse default config and print `bind_ip port` |
| `radixdb-server --config PATH --print-endpoint` | Parse selected config and print `bind_ip port` |
| `radixdb-server --version` | Print build and protocol identity without reading config |
| `radixdb-server --help`, `-h` | Print usage and exit successfully without reading config or starting the server |

The two print-endpoint options may appear in either order. Unknown or conflicting
arguments return the usage line and a non-zero status.

## Configuration selection

There are no command-line overrides for individual server settings and no
environment-variable configuration layer. The TOML file must contain a
top-level `[server]` table and may contain a top-level `[plugins]` package
allowlist. Unknown keys are rejected.

```sh
radixdb-server --config /etc/radixdb/server.toml --print-endpoint
radixdb-server --config /etc/radixdb/server.toml
```

`--print-endpoint` proves TOML decoding only. It does not run complete runtime
validation, admit plugin packages, create `data_dir`, reserve the port or open
a database. A start is ready to accept sessions only after the `listening on`
log line.

## Process behavior

Without an explicit config path, relative paths are resolved from the current
working directory. Within a config, relative `data_dir` is also based on the
process working directory. The packaged systemd unit controls both values.

`SIGTERM` and `SIGINT` request clean shutdown. Success closes sessions and every
opened database and logs `radixdb-server stopped cleanly`. Startup or runtime
failure returns a non-zero process status and prefixes stderr with
`radixdb-server:`.

Before binding the listener, the server atomically admits every package listed
under `[plugins]` and prints a bounded registry summary with package, type,
function, operator, operator-class, planner-support, shadowed-version and
library-byte counts. An empty allowlist reports registry generation zero. Any
package validation failure aborts startup; no partial registry becomes visible.
See [extensions](../../../administration/extensions/).

The server supports database-bound login/password authentication over ordinary
TCP and optional direct TLS. A configured root verifier requires the original
password on every endpoint; without it, passwordless root is restricted to a
loopback plaintext recovery endpoint. Read
[authentication](../../../administration/authentication/) before exposing any
transport. Complete parameters are in the
[server configuration chapter](../../../administration/configuration/).

## radixdb-password

`radixdb-password` generates the Argon2id PHC verifier accepted by
`server.authentication.root_password_verifier`.

```text
radixdb-password [--help | --version]
```

With no option, the program reads one password line from redirected standard
input and writes one verifier line to standard output. The password must be
valid UTF-8 and contain 1 to 1024 bytes. Interactive terminal input is refused
because the utility cannot disable echo itself, and a password is never accepted
as a command-line argument. `--help` and `-h` print usage; `--version` prints the
same build identity fields as the other release programs.

An empty, multiline, oversized or non-UTF-8 input fails without a verifier.
Protect the output as credential material and preserve the complete string,
including every `$` delimiter. The operational procedure is in
[authentication](../../../administration/authentication/).
