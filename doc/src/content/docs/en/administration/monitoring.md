---
title: Monitoring
description: Observe RadixDB process, database readiness, runtime maintenance and host capacity without turning diagnostics into load.
---

Monitoring must answer four separate questions: is the process supervised, is
the listener accepting the expected protocol, is a particular database ready,
and is the storage engine approaching a resource or maintenance limit. One
green signal does not imply the others.

RadixDB 1.2 does not expose an HTTP health or Prometheus endpoint. The verified
interfaces are the service manager and journal, the binary protocol status
methods, SQL `PRAGMA RUNTIME_STATS`, and operating-system resource counters.

## Process and listener

For a systemd installation, begin with the unit and its first current error:

```sh
systemctl is-active radixdb
systemctl show radixdb -p MainPID -p ExecMainStatus -p NRestarts
journalctl -u radixdb -n 100 --no-pager
ss -ltn 'sport = :15441'
```

The bundle's `smoke-client.sh` adds protocol handshake, authentication and build
identity to the listener check. It still does not select an application
database. Alert on repeated restarts, unexpected executable identity, protocol
mismatch and a listener missing from the configured endpoint.

## Server and database status

The Rust client exposes `Connection::server_status()` and
`Connection::database_status(name)`. Their `ServerStatus` result includes build
identity when negotiated, lifecycle, ready state, database entries and bounded
process counters for databases, connections and in-flight frame bytes.

Use global status for process capacity. Use named status only after explicitly
selecting the application database: a database present on disk but not opened
is reported as `Starting` and not ready. Global status can still be `Ready`
because the server accepts sessions while such databases remain unopened.

For an opened database, require all of the following before application traffic:

- named lifecycle is `Ready` and `ready` is true;
- artifact summary has `scan_errors = 0` and `truncated = false`;
- negotiated build identity is the approved server build;
- a cheap representative application read succeeds.

`Emergency` means open, recovery or close failed. Preserve its message and the
server journal before restarting. Artifact inventory is sampled during database
open and cached in the lifecycle registry; ordinary status polling does not walk
the filesystem. Each scan is bounded to 4,096 entries and depth 8, carries a
process-local `sequence` and `sampled_unix_millis`, and omits retained snapshot
trees. Consequently `complete = false` can mean `snapshots_omitted = true`; use
`truncated` and `scan_errors` to distinguish budget exhaustion and I/O failure.

## Engine runtime statistics

`PRAGMA RUNTIME_STATS` returns one TEXT value containing versioned JSON. Run it
through the selected application connection. A local CLI may use it only when
that CLI is the exclusive owner of the file database; never open a
server-owned root concurrently.

```sql
PRAGMA RUNTIME_STATS;
```

The current payload has `format = 2`. Collection uses atomics and try-locks,
does not open payload files, and has declared structural visit limits. Always
store `format`, `sequence`, `captured_unix_millis` and `snapshot_nanos` with the
sample.

First inspect `complete`, `missing_evidence` and `truncated_owners`. When
`complete` is false, affected totals are lower bounds. Do not interpret a zero
from missing or truncated evidence as healthy.

| Area | Primary fields | Operational interpretation |
| --- | --- | --- |
| Transactions | `active_transactions`, `oldest_transaction_age_millis`, `transaction_wait_edges` | Growing age or waits can pin visibility and delay maintenance |
| Hot and staging | `hot_rows`, `hot_bytes`, `staging_transactions`, `staging_rows` | Sustained growth indicates delayed seal, a long transaction or load above maintenance capacity |
| Cold levels | `cold_unleveled_segments`, `cold_l0_segments`, `cold_l0_debt_physical_bytes` | Compare trend with soft/hard limits; nonzero L0 alone is not a failure |
| Compaction | requested/running, active jobs, retry cooldown, backpressure counters | Repeated cooldown or growing hard rejections requires capacity and I/O investigation |
| WAL | `wal_running`, file/max bytes, pending durability, checkpoint time | A ready persistent database should have a running WAL; alert on stalled growth or failed checkpoints |
| Maintenance | worker alive plus seal/compaction/checkpoint calls, failures and timestamps | Alert on failure-counter deltas and no successful progress while work is pending |
| Page cache | `page_cache_warmup` state, target, warmed and resident estimates | Compare with configured level, safe budget and host memory; disabled is valid when configured |
| Storage CPU | configured/effective/in-use/peak workers and leases | Sustained saturation supports tuning, but is not itself corruption |

## Host capacity

Collect filesystem bytes and inodes for the actual database mount, not only `/`.
Also record process RSS, available memory, swap activity and kernel/storage
errors. `du` is a diagnostic sample and can itself create metadata I/O on a
large tree.

```sh
DATA=/opt/radixdb/data
df -B1 "$DATA"
df -i "$DATA"
du -sx --block-size=1 "$DATA/databases"
ps -o pid,etimes,rss,vsz,%cpu,stat,cmd -C radixdb-server
grep -E 'MemAvailable|SwapFree|Dirty|Writeback' /proc/meminfo
journalctl -k -p warning..alert --since '15 minutes ago' --no-pager
```

Trend free bytes and inodes early enough to retain room for WAL, checkpoint,
compaction output and an internal snapshot. Compaction reserves space beyond
its planned output; a `disk_reserve_exhausted` cooldown is an early protection,
not proof that the filesystem is already full.

## Alert policy

Use hard engine limits as hard thresholds and learn rates from the workload.
Useful alerts include:

- named database not ready or lifecycle `Emergency`;
- artifact scan incomplete or any scan error;
- connection or in-flight frame usage approaching configured maxima;
- runtime sample incomplete, truncated or unusually slow;
- WAL not running, durability backlog growing, or checkpoint failures increasing;
- maintenance worker absent while lifecycle is ready;
- L0 debt growing toward a hard limit, repeated retry cooldown, or new hard
  backpressure rejections;
- old transactions and wait edges growing together;
- filesystem byte/inode exhaustion, OOM events or kernel I/O errors.

Monitor deltas for cumulative counters and require persistence across several
samples before paging on trends. Keep one low-rate end-to-end probe that selects
the database and executes a read; process-only checks cannot detect a failed
database open.

See [troubleshooting](../troubleshooting/) for response procedures and
[memory management](../memory/) for RSS and page-cache interpretation.
