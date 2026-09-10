---
title: Configuration Reference
description: Index of server TOML, embedded file DSN and command-line configuration surfaces.
---

RadixDB 1.2 has three separate configuration surfaces. They are not aliases for
one another.

| Surface | Owner | Application |
| --- | --- | --- |
| `server.toml` | `radixdb-server` | Read once at process start; server settings plus an optional native package allowlist |
| `file://...?key=value` | Embedded API and CLI | Read when a file database opens; 39 accepted keys |
| CLI options | `radixdb-cli` | Build a subset of file DSN overrides for that invocation |

The [server configuration](../../administration/configuration/) chapter is the
complete reference for its TOML settings, plugin allowlist, defaults, release
values, ranges and restart rules. The [`radixdb-cli` reference](../programs/cli/)
lists all CLI flags. This page defines the complete file DSN surface.

## File DSN syntax

```text
file:///absolute/path?sync_mode=full&checkpoint_interval=60
```

Keys are case-sensitive ASCII names. Values are not percent-decoded. Each key
may occur once; duplicate and unknown keys are errors. A bare key has an empty
value and normally fails its value parser. `memory://` does not accept file
persistence options.

## Durability and WAL

| Key | Default | Unit or values | Notes |
| --- | ---: | --- | --- |
| `sync_mode` | `normal` | `none`, `normal`, `full`; numeric aliases `0..2` | Durability policy |
| `checkpoint_interval` | `60` | seconds, `u32`; `0` allowed | Periodic checkpoint interval |
| `checkpoint_on_close` | `on` | `on` or `off` | Disable only for controlled crash tests |
| `wal_flush_trigger` | `32768` | bytes, `usize` | Buffered WAL flush threshold |
| `wal_buffer_size` | `65536` | bytes, `usize` | Initial WAL buffer |
| `wal_max_size` | `67108864` | bytes, `usize` | Rotation threshold |
| `sync_interval_ms` | `1000` | milliseconds, `u32` | Normal-mode interval field |
| `wal_compression` | `on` | `on` or `off` | WAL LZ4 |
| `volume_compression` | `on` | `on` or `off` | Data-artifact LZ4 |
| `compression` | `on` | `on` or `off` | Sets both compression switches at this position |
| `keep_snapshots` | `3` | count, `u32` | Physical database snapshot retention |

`compression`, `wal_compression` and `volume_compression` are applied in query
order. Avoid combining the umbrella and individual keys in one DSN.

## Seal, compaction and backpressure

| Key | Default | Unit or values | Notes |
| --- | ---: | --- | --- |
| `compact_threshold` | `4` | segment count, `u32` | Size-tier trigger input |
| `max_compaction_jobs` | `1` | `1..=8` jobs | Distinct tables only |
| `storage_cpu_workers` | `0` | workers, `usize`; `0` auto | Shared CPU-heavy pool |
| `max_compaction_input_segments` | `8` | count, minimum 1 | Per-job work bound |
| `max_compaction_input_bytes` | `536870912` | bytes, minimum 1 | Per-job input bound |
| `max_compaction_output_bytes` | `1073741824` | bytes, minimum 1 | Per-job output bound |
| `compaction_job_time_budget_ms` | `1200000` | milliseconds; `0` disables | Wall-clock budget |
| `compaction_io_bytes_per_sec` | `0` | bytes/second; `0` unlimited | Aggregate average limit |
| `compaction_disk_reserve_bytes` | `1073741824` | bytes; `0` disables | Free-space reserve |
| `compaction_retry_cooldown_ms` | `30000` | milliseconds; `0` disables | Exact-input retry suppression |
| `l0_soft_limit_segments` | `16` | segments, positive | Must be below hard limit |
| `l0_hard_limit_segments` | `32` | segments, positive | Rejects commit before publication |
| `l0_soft_limit_bytes` | `1073741824` | bytes, positive | Must be below hard limit |
| `l0_hard_limit_bytes` | `2147483648` | bytes, positive | Rejects commit before publication |
| `l0_soft_backpressure_wait_ms` | `100` | milliseconds | Maximum cooperative wait per commit |
| `target_volume_rows` | `1048576` | rows, minimum 65536 | Output segment target |
| `seal_hot_bytes_threshold` | `67108864` | bytes, minimum 1 | First seal threshold |
| `seal_incremental_hot_bytes_threshold` | `16777216` | bytes, minimum 1 | Later seal threshold |

The L0 pairs are validated together as `0 < soft < hard`. A value accepted by
the parser is still a workload decision; preserve the defaults until measured.

## Cache and read path

| Key | Default | Unit or values | Notes |
| --- | ---: | --- | --- |
| `page_cache_level` | `0` | `0..=10` | Proactive OS page-cache warmup |
| `page_cache_max_bytes` | `0` | bytes; `0` automatic | Warmup hard cap |
| `page_cache_memory_reserve` | `0` | bytes; `0` automatic | Memory left outside warmup |
| `volume_cache_bytes` | `1073741824` | bytes | Resident cold payload budget |
| `read_queue_depth` | `1` | requests, minimum 1 | Sequential artifact reads |
| `copy_max_transaction_bytes` | `536870912` | bytes, positive | One atomic COPY memory envelope |

`page_cache_*` controls the operating-system cache. Engine-owned
`volume_cache_bytes` is a separate bounded budget and does not form a complete
RSS limit. See [memory management](../../administration/memory/).

## MVCC cleanup

| Key | Default | Unit or values | Notes |
| --- | ---: | --- | --- |
| `cleanup` | `on` | `on` or `off` | Background MVCC cleanup |
| `cleanup_interval` | `60` | seconds, `u64` | Cleanup cycle interval |
| `deleted_row_retention` | `300` | seconds, `u64` | Deleted-row retention |
| `transaction_retention` | `3600` | seconds, `u64` | Completed transaction retention |

`commit_batch_size`, `compression_threshold`, `scan_prefetch_cache_bytes` and
`block_cache_bytes` are rejected retired options. None is part of the 39-key
contract. Choose `sync_mode` for durability; the engine does not advertise an
unimplemented threshold or cache budget.

Configuration is attached when the database first opens. Connections sharing
the same canonical file path share one engine owner and must not assume a later
DSN silently reconfigures it. Close every owner before changing persistent
runtime settings, and never edit storage artifacts directly.
