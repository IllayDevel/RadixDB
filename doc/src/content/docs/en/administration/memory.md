---
title: Memory Management
description: Memory budgets, process RSS, operating-system page cache and measured profiles for RadixDB 1.2.
---

RadixDB does not load an entire database into the process before serving it.
Cold payloads can remain on disk and are decoded in bounded groups, while hot
MVCC state, metadata, query work and selected cache content consume memory as
needed. The configured budgets control individual owners; their sum is not a
hard limit on server RSS.

This chapter describes the documentation baseline
`23bf35df011aae6816d77578be96074b02bc363c`. Memory measurements below belong
to their recorded revisions and workloads, not automatically to this binary or
every production workload.

## Accounted memory owners

The release configuration exposes these principal bounds:

| Owner | Release value | Scope | Hard RSS limit? |
| --- | ---: | --- | --- |
| Hot state, first seal trigger | `67108864` bytes | Approximate per-table maintenance trigger | No |
| Hot state, later seal trigger | `16777216` bytes | Approximate per-table trigger after cold data exists | No |
| One atomic `COPY FROM` | `536870912` bytes | Estimated row, MVCC and WAL amplification for one statement | No |
| In-flight protocol frames | `268435456` bytes | Process-wide admission budget | No |
| OS page-cache warmup | disabled (`page_cache_level = 0`) | Per database, file pages owned by the OS | No, and not process RSS |

The embedded `PersistenceConfig` additionally defaults
`volume_cache_bytes` to `1073741824`. It accounts evictable materialized cold
column payloads, but excludes row IDs, zone maps, descriptors and other metadata
needed for routing. The server does not expose this value in `server.toml`; each
opened server database receives the engine default.

Budget scope matters. Per-database values can be multiplied by concurrently
opened databases. Per-table seal triggers can be crossed by several tables, and
several sessions can own query results, transaction state and cursor buffers.
Allocator arenas, thread stacks, catalog state, indexes, WAL buffers and
temporary compaction work remain outside the table above.

## Retired cache placeholders

The engine does not currently provide separate compressed-block or decoded
scan-prefetch caches. The former placeholder settings `block_cache_bytes` and
`scan_prefetch_cache_bytes` were removed from `PersistenceConfig`, the file DSN,
server TOML, release templates and runtime statistics. Supplying either key now
fails closed as an unknown option. This keeps the configuration surface honest;
any future cache must return with real admission, accounting and A/B evidence.

## Operating-system page cache

Page-cache warmup is a real, separate mechanism. A database-local worker reads
members of the pinned immutable generation through a reusable 1 MiB buffer and
lets the operating system own residency and eviction. It does not copy the
generation into an engine-owned heap cache, pin pages, or make cache residency a
correctness requirement.

`page_cache_level` requests tenths of the current generation: level 1 requests
about 10 percent and level 10 requests all generation bytes. The target is the
smallest of the requested fraction, available memory minus reserve, and a
positive `page_cache_max_bytes`. `page_cache_memory_reserve` sets that reserve;
zero selects the automatic value, which is the larger of 256 MiB and 10 percent
of detected available memory. Host `MemAvailable` is further bounded by the
current cgroup-v2 allowance when both are available. If neither source is
available, automatic warmup chooses zero rather than guessing.

Metadata is warmed first, then indexes, then recently accessed data. Generation
replacement can reuse unchanged files. A completed warmup means the requested
bytes were read; the OS may evict them immediately afterwards.

Inspect or request warmup outside an explicit transaction:

```sql
PRAGMA PAGE_CACHE_STATUS;
PRAGMA PAGE_CACHE_WARMUP;
PRAGMA PAGE_CACHE_WARMUP_WAIT = 300000;
```

The result is one JSON value. Check `state`, `total_generation_bytes`,
`safe_budget_bytes`, `target_bytes`, `warmed_bytes`,
`resident_estimate_bytes`, `limited_by` and `last_error`. With release level 0,
state is `disabled` and no generation inventory is read.

## Can the whole database fit in cache?

Do not compare database bytes directly with total host RAM. First obtain the
current generation size from a completed `PAGE_CACHE_STATUS`, then reserve
memory for the process, the operating system and workload peaks. A level-10
request can cover the generation only when:

```text
generation_bytes <= page_cache_max_bytes (when nonzero)
generation_bytes <= detected_available_memory - effective_reserve
```

That estimate is still not a promise. During warmup, process RSS and the OS file
cache compete for physical memory; other services and later queries can evict
pages. Snapshots, WAL, staging and superseded generations also consume disk and
may contribute file pages without belonging to the current warmup target.

For a dedicated host, start with level 0. Measure peak workload RSS and host
`MemAvailable`, choose an explicit reserve larger than the observed process and
system headroom, then test a capped low level before increasing it. Never use
swap activity or OOM pressure as a cache-eviction policy.

## Observing the engine and process

Engine statistics expose logical owners; the operating system supplies process
and host measurements:

```sql
PRAGMA VOLUME_STATS;
PRAGMA RUNTIME_STATS;
```

`VOLUME_STATS.memory_bytes` includes each segment's currently resident storage
representation and breaks out metadata, row IDs, index data, descriptors and
column payloads. `RUNTIME_STATS` provides totals for hot/cold state, configured
cache budgets, page-cache warmup and storage workers. It is a bounded snapshot;
`complete = false` lists owners that could not be sampled without blocking.

On a systemd installation, sample the process and host separately. `ps` reports
RSS in KiB:

```sh
pid="$(systemctl show radixdb --property MainPID --value)"
ps -o pid=,rss=,vsz=,nlwp=,cmd= -p "$pid"
awk '/MemAvailable|Cached|SwapFree/ { print }' /proc/meminfo
```

Record idle after reopen, peak during the real workload, and quiescent RSS after
the workload. One final sample cannot show a transient peak. Use cgroup or
service accounting for continuous production observation.

## Recorded profiles

The following results deliberately remain separate:

| Profile | Identity and conditions | Result |
| --- | --- | --- |
| Small clean reopen | `1c604d34`, Linux/NVMe, 20,000 rows, 120 tables, 358 indexes, one storage worker, page cache and prefetch/block-cache budgets disabled | Median RSS `24571904` B (23.4 MiB) with system allocator; `56848384` B (54.2 MiB) with default mimalloc |
| Six-hour endurance | `dd0bf75c`, 100 million seeded rows, 16..256 clients, 1.76 GiB RAM, 5400 rpm HDD, competing I/O and an observed SATA reset | Peak server RSS `1060020224` B (1010.9 MiB); final `206327808` B (196.8 MiB) |

The small profile measures a reopened database, not creation and indexing. The
six-hour profile includes a different revision and intentionally adverse I/O;
it is reliability evidence, not a latency or memory SLA. Neither result includes
OS page-cache bytes in process RSS.

## Tuning order

1. Keep the release configuration and record an idle/reopen baseline.
2. Run the representative concurrency, query and ingest mix while sampling RSS,
   cgroup pressure, swap and `RUNTIME_STATS`.
3. Reduce open-database and connection concurrency before treating individual
   cache values as a global limit.
4. If hot memory dominates, review table-level seal thresholds together with
   checkpoint and compaction behavior.
5. Enable page-cache warmup only with an explicit reserve and cap, then compare
   cold and warm latency against host pressure.
6. Change one group of values per run, restart the server and retain the rollback
   configuration.

See [storage architecture](../storage/) for row groups and maintenance, and
[server configuration](../configuration/) for accepted values and restart
semantics.
