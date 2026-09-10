---
title: Operating the Server
description: Start, inspect and stop radixdb-server while preserving database ownership and readiness boundaries.
---

`radixdb-server` owns file databases and exposes the RadixDB binary protocol over
TCP. A client connects, completes the handshake and authentication, and selects
a named database before executing SQL. The server opens databases on demand and
keeps them in its process registry until shutdown.

Do not open a server-owned database concurrently through embedded code or
`radixdb-cli`. The CLI is a local embedded/file tool, not a TCP shell.

## Minimal configuration

The configuration file contains a required top-level `[server]` table and may
contain an optional `[plugins]` table. Unknown fields are rejected. This is the
smallest practical configuration:

```toml
[server]
bind_ip = "127.0.0.1"
port = 15441
data_dir = "data"
```

Relative `data_dir` paths are resolved from the process working directory, not
from the directory containing `server.toml`. The packaged systemd unit sets the
working directory to the installation prefix, so its `data` path is stable. The
[server configuration](../configuration/) chapter lists all settings, the
plugin allowlist, defaults and validation rules.

The minimal configuration omits a root password verifier and therefore permits
passwordless `root` only on this plaintext loopback endpoint. For normal
administration, generate an Argon2id verifier with `radixdb-password`, add it as
`server.authentication.root_password_verifier`, and restart. This requires the
original password on every endpoint and permits an intentional non-loopback
bind; use direct TLS unless the complete network path is trusted. Applications
should authenticate as database Principals rather than as `root`.

Inspect the endpoint without opening a listener:

```sh
radixdb-server --config server.toml --print-endpoint
```

The output contains the configured host and port separated by a space.
`--print-endpoint` parses the file, but does not admit configured plugin
packages or prove that the port is free, the data directory is writable or the
server can start.

## Direct start

For a foreground test, run:

```sh
radixdb-server --config server.toml
```

After configuration validation, the process admits the complete plugin
allowlist before binding. It first writes a registry summary such as:

```text
radixdb-server plugin registry generation=0 packages=0 types=0 functions=0 operators=0 operator_classes=0 planner_support=0 shadowed_versions=0 library_bytes=0
```

An invalid package aborts startup atomically. After data-directory creation and
a successful bind, the process writes:

```text
radixdb-server listening on 127.0.0.1:15441
```

This line proves listener readiness only. It does not mean that any particular
database has completed open or recovery.

Without arguments the executable reads `server.toml` from its working directory.
`--version` prints identity without reading configuration or starting a listener.
Do not combine `--version` with another option.

## Database selection and files

The wire protocol has no separate `CREATE DATABASE` command. After handshake and
authentication, `select_database(name)` opens an existing database or creates:

```text
<data_dir>/databases/<name>/
```

A name must contain 1 to `max_database_name_bytes` bytes and only ASCII letters,
digits, `_` or `-`. The packaged default limit is 64 bytes. A connection cannot
switch databases while it owns an explicit transaction or an open cursor.

Opening is lazy. Logs distinguish `state=opening`, `state=ready` and
`state=failed`. A failed open remains visible for diagnosis and becomes eligible
for a bounded retry after the underlying problem is repaired. The configured
`max_databases` limits entries in the process registry, including diagnostic
states.

## Readiness checks

For an installed bundle, begin with the supplied protocol probe:

```sh
cd /opt/radixdb
./smoke-client.sh
```

A successful result has this shape:

```text
ready version=1.1.0 protocol=17 state=Ready
```

This confirms the listener, handshake, authentication, server lifecycle and
negotiated build identity. It does not select an application database.

Applications should call `database_status(name)` and wait for `ready = true`
after selecting or recovering a database, then execute a cheap application read.
`server_status()` describes process lifecycle; `database_status()` additionally
reports database state and artifact counters. Treat a TCP connect and the
listener log line as necessary, not sufficient, readiness signals.

## systemd operation

The installed unit runs the server as `radixdb:radixdb`, with
`WorkingDirectory=/opt/radixdb` and write access limited to its data, log and run
directories. Routine commands are:

```sh
sudo systemctl status radixdb --no-pager
sudo systemctl restart radixdb
journalctl -u radixdb -n 100 --no-pager
```

The unit uses `Restart=on-failure`, a two-second restart delay and a 60-second
stop window. A clean operator stop does not count as a failure. The authoritative
unit is the `radixdb.service` file from the verified bundle; review generated
paths after using a non-default installation prefix.

## Clean shutdown and restart

`SIGTERM` and `SIGINT` request a clean shutdown. The server cancels active
requests, closes session sockets, waits for workers and then closes every opened
database. A successful foreground shutdown ends with:

```text
radixdb-server stopped cleanly
```

Use the service manager for an installed process:

```sh
sudo systemctl stop radixdb
sudo systemctl start radixdb
```

The release lifecycle wrappers record both PID and Linux process start time,
verify the executable and listener, serialize transitions with `flock`, and
refuse to signal a stale or foreign PID. They are useful in the bundle sandbox;
systemd remains the process owner after installation.

A checked restart test confirms that committed data survives reopening and a
rolled-back row stays absent. That is a functional restart check, not a backup.
Keep independent backups and test restore procedures for important data.

## Startup failures

Before changing configuration, check the first error in the journal:

- a missing config path fails before bind;
- an unknown key or invalid value is rejected;
- a relative data path may resolve somewhere unexpected when the working
  directory is wrong;
- a non-writable data directory prevents startup;
- an occupied port prevents bind;
- TLS startup fails when certificate material is missing, invalid or has unsafe
  private-key permissions.
- plugin startup fails when a listed path, manifest, checksum, platform,
  descriptor, dependency or filesystem ownership check is invalid.

A non-loopback bind is permitted. Protect it with direct TLS or a trusted
network and authenticate applications as database Principals. Administrative
`root` requires a configured password verifier there; without one, only the
plaintext loopback recovery path is available.

After correcting the cause, start the service again and repeat both the protocol
probe and application-database readiness check. See [installing the server](../installation/)
for the bundle and filesystem layout, or follow the [server tutorial](../../tutorial/server-connection/)
for an isolated foreground exercise.

Native package placement, exact-version bindings and recovery from a missing
package are covered in [extensions](../extensions/).
