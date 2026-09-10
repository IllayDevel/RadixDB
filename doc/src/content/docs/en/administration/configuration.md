---
title: Server Configuration
description: Select, validate and apply server.toml settings and the native plugin allowlist.
---

`radixdb-server` reads one TOML document with a required top-level `[server]`
table and an optional top-level `[plugins]` table. Configuration controls the
listener, process limits, native package allowlist and the storage settings
copied into each database when it opens. Unknown tables and keys are errors; a
misspelled limit is never silently ignored.

## Selecting the file

The server has two file-selection rules:

1. `--config PATH` reads exactly `PATH`.
2. Without `--config`, it reads `server.toml` from the process working directory.

There is no server environment variable or second configuration file that
overrides individual TOML values. Environment variables beginning with
`RADIXDB_RELEASE_` belong to the release lifecycle scripts, not to the server
configuration parser. The systemd unit always passes the installed absolute
config path.

```sh
radixdb-server --config /etc/radixdb/server.toml
radixdb-server --config /etc/radixdb/server.toml --print-endpoint
```

`--print-endpoint` verifies TOML decoding and prints `bind_ip port`; it does not
run the complete runtime validation, admit plugin packages, test filesystem
permissions or reserve the port. A real foreground start or service restart is
the final startup check.

## Release template

The 1.2 release bundle supplies this complete template. All sizes are integer
bytes; suffixes such as `MiB` and `GiB` are not accepted values.

```toml
[server]
bind_ip = "127.0.0.1"
port = 15441
data_dir = "data"
max_connections = 64
max_inflight_frame_bytes = 268435456
max_databases = 64
max_database_name_bytes = 64
connect_timeout_secs = 10
connection_idle_timeout_secs = 28800
net_read_timeout_secs = 30
net_write_timeout_secs = 60
cursor_batch_max_rows = 1024
cursor_batch_max_bytes = 8388608
max_frame_bytes = 67108864
copy_max_transaction_bytes = 536870912
max_compaction_jobs = 1
storage_cpu_workers = 0
page_cache_level = 0
page_cache_max_bytes = 0
page_cache_memory_reserve = 0
target_volume_rows = 1048576
seal_hot_bytes_threshold = 67108864
seal_incremental_hot_bytes_threshold = 16777216
read_queue_depth = 1

# [server.authentication]
# root_password_verifier = "$argon2id$..."

# [plugins]
# package_directories = ["/opt/radixdb/plugins/radix-spatial-1.0.0"]
```

Plaintext login/password TCP is the default and requires no certificate. To
enable direct TLS, add the optional nested transport table:

```toml
[server.transport]
mode = "tls"
certificate_chain = "/etc/radixdb/tls/server-chain.pem"
private_key = "/etc/radixdb/tls/server-key.pem"
```

TLS mode requires both files and rejects a private key readable by group or
other users. Omit `[server.transport]` (or select `mode = "plaintext"`) for an
ordinary password-authenticated TCP endpoint.

To require a password for the administrative `root` login, add the optional
authentication table using the complete output of `radixdb-password`:

```toml
[server.authentication]
root_password_verifier = "$argon2id$..."
```

The server accepts only a valid Argon2id PHC verifier within its supported
security bounds. The value is redacted from debug output, but remains credential
material and must be protected by filesystem permissions. When this setting is
present, passwordless `root` is disabled on every endpoint. When it is absent,
passwordless recovery is available only on plaintext loopback. The
[authentication chapter](../authentication/) gives the generation, login and
rotation procedure.

Native extensions use an explicit startup allowlist:

```toml
[plugins]
package_directories = [
  "/opt/radixdb/plugins/radix-spatial-1.0.0",
]
```

Each value names one exact package directory, not a parent directory to scan.
The path must be absolute, normalized and free of symbolic links. An omitted or
empty table loads no plugin files and produces registry generation zero. The
server admits every listed package before binding the listener; one invalid
entry aborts the whole startup. See [Extensions](../extensions/) for ownership,
package format and version replacement rules.

For the standard installation, `WorkingDirectory=/opt/radixdb`; therefore the
relative data path resolves to `/opt/radixdb/data`. Use an absolute path when the
working directory is not controlled by the service definition.

## Parameter reference

The code default applies only when an optional key is omitted. The release value
is explicitly present in the bundled `server.toml` and therefore takes
precedence. The three required keys have no code default.

| Key | Code default | Release value | Valid values | Applies to |
| --- | ---: | ---: | --- | --- |
| `bind_ip` | required | `127.0.0.1` | valid IP address | Listener and authentication boundary |
| `port` | required | `15441` | `1..=65535` | Listener |
| `data_dir` | required | `data` | non-empty UTF-8 path without `?` | Database root |
| `transport` | `plaintext` | omitted (`plaintext`) | `plaintext` or TLS table | Socket encryption policy |
| `authentication.root_password_verifier` | absent | absent | Argon2id PHC string | Optional `root` password verifier |
| `plugins.package_directories` | empty | omitted | exact absolute package paths | Startup native plugin registry |
| `max_connections` | `151` | `64` | `> 0` | Process admission |
| `max_inflight_frame_bytes` | `268435456` | `268435456` | `> 0`, at least `max_frame_bytes` | Process frame budget |
| `max_databases` | `64` | `64` | `> 0` | Process database registry |
| `max_database_name_bytes` | `64` | `64` | `> 0` | Database selection |
| `connect_timeout_secs` | `10` | `10` | `> 0` seconds | Initial handshake and authentication |
| `connection_idle_timeout_secs` | `28800` | `28800` | `> 0` seconds | Idle session |
| `net_read_timeout_secs` | `30` | `30` | `> 0` seconds | Frame payload read |
| `net_write_timeout_secs` | `60` | `60` | `> 0` seconds | Socket write |
| `cursor_batch_max_rows` | `1024` | `1024` | `> 0` rows | Legacy row batch |
| `cursor_batch_max_bytes` | `8388608` | `8388608` | `> 0`, at most `max_frame_bytes` | Cursor batch |
| `max_frame_bytes` | `67108864` | `67108864` | `256..=max_inflight_frame_bytes` bytes | Binary protocol frame |
| `copy_max_transaction_bytes` | `536870912` | `536870912` | `> 0` bytes | One atomic `COPY FROM` |
| `max_compaction_jobs` | `1` | `1` | `1..=8` | Concurrent table-local compactions |
| `storage_cpu_workers` | `0` | `0` | `>= 0` workers | Shared seal/compaction CPU pool |
| `page_cache_level` | `0` | `0` | `0..=10` | OS page-cache warmup |
| `page_cache_max_bytes` | `0` | `0` | `>= 0` bytes | Warmup cap |
| `page_cache_memory_reserve` | `0` | `0` | `>= 0` bytes | Memory excluded from warmup |
| `target_volume_rows` | `1048576` | `1048576` | `>= 65536` rows | New cold-volume shape |
| `seal_hot_bytes_threshold` | `67108864` | `67108864` | `> 0` bytes | First hot-buffer seal |
| `seal_incremental_hot_bytes_threshold` | `16777216` | `16777216` | `> 0` bytes | Later incremental seals |
| `read_queue_depth` | `1` | `1` | `> 0` requests | Sequential cold reads |

## Listener and protocol limits

The release chooses 64 simultaneous connections even though omission of the
key uses the code default 151. Raising the limit also raises the number of
sessions that can compete for frame, cursor and storage resources; it is not an
isolated throughput switch.

`max_inflight_frame_bytes` is one process-wide admission budget for concurrent
frame payloads. Each `max_frame_bytes` must fit inside it, and each cursor batch
must fit inside one frame. The 256-byte lower frame bound keeps protocol control
messages representable. These are memory and protocol safety limits, not query
result limits: clients can fetch a cursor in several batches.

The four timeouts describe different phases. `connect_timeout_secs` covers
initial setup and authentication. Once connected, an idle session can remain
without a new frame for `connection_idle_timeout_secs`; a partial frame uses
`net_read_timeout_secs`, and writes use `net_write_timeout_secs`.

## Storage and memory parameters

Storage settings are copied into a database when the server opens it. They are
not retroactively injected into an already-open engine.

`copy_max_transaction_bytes` is a conservative budget for one atomic `COPY`; it
includes estimated MVCC and WAL amplification and is not a process RSS limit.
`target_volume_rows` affects newly produced immutable volume shape, so change it
as a storage-layout decision before a major load.

`storage_cpu_workers = 0` uses the CPUs visible to the process or cgroup. A
positive value caps the shared seal/compaction pool. `max_compaction_jobs`
controls concurrent jobs for distinct tables and never allows two owners for the
same table. Keep the portable default 1 until the target disk and workload have
been measured.

Page-cache warmup is disabled at level 0. Level 10 requests the complete current
generation only within the effective cap and memory reserve; warmup remains an
optimization and is never required for correctness. `page_cache_max_bytes = 0`
and `page_cache_memory_reserve = 0` select automatic policies. Queries, MVCC
state, metadata and the OS page cache remain outside any single budget.

`read_queue_depth = 1` is the portable release setting. A higher value can help
measured sequential reads on suitable storage, but random primary-key paths stay
at depth 1 and no value is a universal accelerator.

## Applying a change

The server reads configuration only during process startup. Editing the file
does not reload listener or storage settings. Use this sequence:

1. Preserve the working configuration.
2. Edit one related group of values and check TOML decoding with
   `--print-endpoint`.
3. Restart the server, then inspect the journal for full validation or open
   failures.
4. Run the protocol smoke and wait for every application database to report
   ready.
5. Restore the previous file and restart again if validation or workload checks
   fail.

```sh
sudo systemctl restart radixdb
sudo systemctl status radixdb --no-pager
cd /opt/radixdb && ./smoke-client.sh
```

Changing listener values disconnects clients as part of restart. Changing
storage or plugin values also requires databases to close and reopen, which the
clean server shutdown performs. Do not edit database artifacts to apply a
setting.

The [installation chapter](../installation/) defines ownership and replacement
of the config file. [Server operation](../server/) explains readiness and clean
restart. Storage and memory chapters provide workload-oriented tuning guidance;
until they are published, keep the release values unless a measured test justifies
the change.
