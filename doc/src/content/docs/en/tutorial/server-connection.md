---
title: Connecting to a Server
description: Run a separate local server and connect with the supplied TCP client example.
---

So far, the CLI has opened the database inside its own process. In server mode,
the server owns the database files and applications send requests over TCP.
`radixdb-cli` is not a TCP shell. This exercise runs an existing client program;
you do not need to write Rust code or use an ORM.

Use the same Linux source checkout and toolchain as in
[Getting Started](../getting-started/), with two terminal windows at the
repository root. The example uses a new database, not the employees database
from previous chapters. Do not open the server's files concurrently with the
local CLI.

## Build the Programs

In the first terminal:

```sh
cargo build --locked --bin radixdb-server
./target/debug/radixdb-server --version
```

Build the client and server from the same source revision. The public client
workspace has its own committed lockfile, so build it without changing the
dependency set:

```sh
cargo build --locked \
  --manifest-path examples/public/rust-client/Cargo.toml \
  --bin basic
```

The verified RadixDB 1.2 source baseline uses protocol 17. Confirm the server
identity before starting it; a local source build currently reports application
version 1.1.0 together with its full Git revision and protocol version.

## Start a Local Server

Check that port 15443 is unused:

```sh
ss -ltn 'sport = :15443'
```

There should be no listener row below the heading. If the port is occupied,
choose another unused port and change it in both the configuration and client
command; do not stop an unrelated service.

Create a separate temporary data directory and configuration:

```sh
server_dir=$(mktemp -d /tmp/radixdb-server-tutorial.XXXXXX) || exit 1
cat > "$server_dir/server.toml" <<EOF
[server]
bind_ip = "127.0.0.1"
port = 15443
data_dir = "$server_dir/data"
EOF
./target/debug/radixdb-server --config "$server_dir/server.toml"
```

Wait for `radixdb-server listening on 127.0.0.1:15443`. Leave this terminal
running. The message confirms a listener, not that a particular database has
been opened successfully. Database opening happens when the client selects it.

Keep this exercise on loopback. Because its configuration omits a root verifier,
the server accepts passwordless `root` only on this plaintext loopback endpoint.
Normal applications authenticate as catalog Principals with `CONNECT`;
administrators can configure a root password, and a direct TLS endpoint is
available when credentials must cross an untrusted network. Review
[authentication](../../administration/authentication/) and
[access control](../../administration/access-control/) before deployment.

## Connect and Query

In the second terminal, from the same repository root:

```sh
env -u RADIXDB_PASSWORD RADIXDB_LOGIN=root \
  ./examples/public/rust-client/target/debug/basic \
  127.0.0.1:15443 manual_notes
```

The command explicitly avoids inheriting an unrelated password from the shell.
The example connects, authenticates, selects `manual_notes`, creates a table,
inserts two rows and reads them through a cursor. Expected output:

```text
inserted rows: 1
last insert id: 1
notes:
  1 | first note | false
  2 | second note | true
```

Selecting the database creates or opens
`<server_dir>/data/databases/manual_notes`. The example drops and recreates
`rt_client_notes` on every run: never point it at a database with important
data. The temporary directory makes this exercise independent of other databases.

The client exits after closing its connection. The server continues running;
client connection shutdown and server shutdown are different operations.

## Stop the Server

Return to the first terminal and press Ctrl+C. Wait for
`radixdb-server stopped cleanly` and the shell prompt. The files remain in the
temporary directory. Only after shutdown, inspect `server_dir` and remove that
specific directory if it is no longer needed. Do not delete files from under
a running server.

A connection refusal usually means the process is not listening at the
specified address. Check the first terminal and the port; do not change the
bind address to a public interface to bypass a local startup problem.

Continue with [client interfaces](../../clients/overview/) to choose between
embedded access, a TCP connection and the ORM.
