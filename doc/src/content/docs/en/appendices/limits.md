---
title: Limits
description: Structural ceilings, configurable admission limits and measured scale for RadixDB 1.2.
---

RadixDB does not publish one maximum database size or row count. Capacity is
bounded by several independent format ceilings, runtime admission settings,
available memory, file descriptors and storage. Multiplying the largest values
below does not produce a supported deployment size.

This appendix separates three kinds of evidence:

- **Hard limit**: a format or semantic ceiling enforced by the 1.2 code.
- **Configurable limit**: an operator-controlled admission or resource setting.
- **Measured scale**: one completed workload under recorded conditions, not a
  hard limit, capacity promise or latency SLA.

Unless a row says otherwise, source limits were checked at
`23bf35df011aae6816d77578be96074b02bc363c`. MiB and GiB use powers of 1024.
The complete operator surface is in the [configuration reference](../../reference/configuration/).

## Catalog and semantic ceilings

| ID | Limit | Value and unit | Enforcement condition | Source and SHA |
| --- | --- | ---: | --- | --- |
| HARD-01 | Catalog file | 536,870,912 bytes (512 MiB) | A larger catalog pack is rejected before decode | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-02 | Objects in one catalog | 262,144 objects | Applies to one decoded catalog generation | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-03 | Edges in one catalog | 1,048,576 edges | Applies to catalog dependency edges | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-04 | Fields in one catalog object | 64 fields | The object codec rejects a larger field directory | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-05 | Payload of one catalog object | 16,777,216 bytes (16 MiB) | Encoded object payload, not total catalog size | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-06 | Normalized name | 1,024 bytes | UTF-8 byte length after normalization | `crates/radixdb-catalog/src/name.rs` @ `23bf35df` |
| HARD-07 | Display name | 4,096 bytes | UTF-8 byte length of the preserved spelling | `crates/radixdb-catalog/src/name.rs` @ `23bf35df` |
| HARD-08 | Canonical SQL text | 16,777,216 bytes (16 MiB) | Per catalog-owned canonical SQL value | `crates/radixdb-catalog/src/payload/common.rs` @ `23bf35df` |
| HARD-09 | Catalog dependency depth | 256 edges | Validation rejects a deeper dependency graph | `crates/radixdb-catalog/src/graph/validate.rs` @ `23bf35df` |
| HARD-10 | Routine arguments | 1,024 arguments | Per stored routine definition | `crates/radixdb-catalog/src/payload/routine.rs` @ `23bf35df` |
| HARD-11 | Routine result columns | 4,096 columns | Per stored routine result definition | `crates/radixdb-catalog/src/payload/routine.rs` @ `23bf35df` |
| HARD-12 | Job arguments | 1,024 arguments | Per stored job definition | `crates/radixdb-catalog/src/payload/job.rs` @ `23bf35df` |
| HARD-13 | One job literal | 8,388,608 bytes (8 MiB) | Per encoded job argument value | `crates/radixdb-catalog/src/payload/job.rs` @ `23bf35df` |
| HARD-14 | Vector dimensions | 65,535 dimensions | Per catalog vector type declaration | `crates/radixdb-catalog/src/payload/data_type.rs` @ `23bf35df` |
| HARD-15 | Navigable-reference depth | 8 steps | Per expanded path in one read-only query | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |
| HARD-16 | Navigable-reference paths | 256 paths | Per query expansion | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |
| HARD-17 | Navigable-reference edges | 512 edges | Across expanded paths in one query | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |

These ceilings describe admitted definitions and query expansion. They do not
mean that a schema near every ceiling will fit the process memory budget.

## Native extension ceilings

These limits belong to plugin ABI 1.0 and the 1.2 startup loader. They are not
permission to approach every maximum in one package.

| ID | Limit | Value and unit | Enforcement condition | Source and SHA |
| --- | --- | ---: | --- | --- |
| PLUG-01 | Package manifest | 65,536 bytes (64 KiB) | Checked before TOML decode | `crates/radixdb-plugin-host/src/manifest.rs` @ `40b1b3d1` |
| PLUG-02 | Package shared library | 268,435,456 bytes (256 MiB) | Empty or larger `.so` is rejected | `crates/radixdb-plugin-host/src/manifest.rs` @ `40b1b3d1` |
| PLUG-03 | Stable local ID | 255 bytes | UTF-8, nonempty and without NUL | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-04 | One external value | 16,777,216 bytes (16 MiB) | Type descriptor can declare a lower `max_bytes` | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-05 | Entries in one descriptor table | 65,535 entries | Applies independently to each descriptor array | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-06 | Native function arguments | 1,024 arguments | Per descriptor signature | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-07 | Planner candidate spans | 4,096 spans | A support descriptor can declare a lower maximum | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-08 | Hash components | 256 components and 65,536 bytes | Per semantic hash callback | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-09 | Plugin diagnostic detail | 4,096 bytes | Longer UTF-8 detail is bounded before host mapping | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |

The initial package ABI accepts only x86-64 little-endian ELF for
`x86_64-unknown-linux-gnu`, with glibc no newer than 2.36. This is a platform
boundary, not a byte-count ceiling. See
[Installing and operating extensions](../../administration/extensions/) for
the complete admission contract.

## Storage-format ceilings

| ID | Limit | Value and unit | Enforcement condition | Source and SHA |
| --- | --- | ---: | --- | --- |
| HARD-18 | CONTROL record | 4,096 bytes | Fixed record size; not a tunable allocation | `crates/radixdb-storage/src/v6/control.rs` @ `23bf35df` |
| HARD-19 | Tables in one database manifest | 262,144 tables | Per database generation | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-20 | Segments in one table manifest | 1,048,576 segments | Per table generation | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-21 | Manifests admitted during open | 262,145 manifests | One database manifest plus table manifests | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-22 | Segments admitted during open | 4,194,304 segments | Sum across an opened database generation | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-23 | Metadata admitted during open | 536,870,912 bytes (512 MiB) | Accounted decoded metadata for one generation | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-24 | Reachable identities | 8,388,608 identities | Nodes visited while validating one generation | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-25 | Reachability work | 1,073,741,824 bytes (1 GiB) | Accounted reachability-validation budget | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-26 | One immutable artifact file | 68,719,476,736 bytes (64 GiB) | File-layout ceiling for DATA or INDEX artifacts | `crates/radixdb-storage/src/v6/artifact.rs` @ `23bf35df` |
| HARD-27 | Rows in one DATA artifact | 4,294,967,295 rows | `u32::MAX`; not a per-table or per-database limit | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-28 | Columns in one table artifact | 4,096 columns | Checked in the DATA header | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-29 | Row groups in one DATA artifact | 65,536 groups | Checked in the DATA header | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-30 | Rows in one row group | 65,536 rows | Capacity check for DATA row groups | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-31 | Blocks in one DATA artifact | 4,194,304 blocks | DATA directory ceiling | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-32 | Stored bytes in one DATA block | 268,435,456 bytes (256 MiB) | Compressed/on-disk block payload | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-33 | Logical bytes in one DATA block | 536,870,912 bytes (512 MiB) | Decoded block payload | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-34 | One variable-length value | 268,435,456 bytes (256 MiB) | Per value before DATA encoding | `crates/radixdb-storage/src/v6/data/column.rs` @ `23bf35df` |
| HARD-35 | Accelerators in one INDEX artifact | 4,096 accelerators | Exact, ordered and vector structures share this directory | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-36 | Sections in one INDEX artifact | 16,384 sections | INDEX directory ceiling | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-37 | Pages in one INDEX artifact | 4,194,304 pages | Sum across accelerators | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-38 | Key columns in one index | 64 columns | Per index accelerator | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-39 | Entries in one INDEX page | 1,048,576 entries | Directory count, also bounded by page bytes | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-40 | Stored bytes in one INDEX page | 67,108,864 bytes (64 MiB) | Compressed/on-disk page payload | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-41 | Logical bytes in one INDEX page | 268,435,456 bytes (256 MiB) | Decoded page payload | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-42 | Catalog WAL replay bytes | 1,073,741,824 bytes (1 GiB) | Per recovery replay admission | `crates/radixdb-storage/src/v6/catalog_wal.rs` @ `23bf35df` |
| HARD-43 | Catalog WAL replay transactions | 262,144 transactions | Per recovery replay admission | `crates/radixdb-storage/src/v6/catalog_wal.rs` @ `23bf35df` |
| HARD-44 | Members in one snapshot | 8,388,608 members | Per snapshot manifest | `crates/radixdb-storage/src/v6/snapshot/model.rs` @ `23bf35df` |
| HARD-45 | Snapshot manifest | 805,306,672 bytes | Encoded manifest ceiling for the maximum member directory | `crates/radixdb-storage/src/v6/snapshot/model.rs` @ `23bf35df` |

An artifact ceiling protects decoding and allocation. Normal operating targets
are deliberately smaller and are shaped by sealing, compaction and cache
settings.

## Configurable runtime limits

This is a capacity-oriented subset of `release/server.toml`, not a second
configuration reference. A server reads these settings at startup.

| ID | Setting | Release value | Accepted boundary or meaning | Source and SHA |
| --- | --- | ---: | --- | --- |
| CFG-01 | `max_connections` | 64 connections | Positive process admission; code default is 151 when omitted | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-02 | `max_inflight_frame_bytes` | 268,435,456 bytes (256 MiB) | Positive process-wide frame payload budget | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-03 | `max_databases` | 64 databases | Positive entries in the process database registry | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-04 | `max_database_name_bytes` | 64 bytes | Positive UTF-8 byte admission for a selected name | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-05 | `cursor_batch_max_rows` | 1,024 rows | Positive row count per cursor response batch | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-06 | `cursor_batch_max_bytes` | 8,388,608 bytes (8 MiB) | Positive and no greater than `max_frame_bytes` | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-07 | `max_frame_bytes` | 67,108,864 bytes (64 MiB) | At least 256 bytes and no greater than the in-flight budget | `release/server.toml`, `crates/radixdb-protocol/src/lib.rs`, `src/server/config.rs` @ `23bf35df` |
| CFG-08 | `copy_max_transaction_bytes` | 536,870,912 bytes (512 MiB) | Positive memory envelope for one atomic `COPY FROM` | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-09 | `max_compaction_jobs` | 1 job | Accepted range is 1 through 8 jobs | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-10 | `storage_cpu_workers` | 0 workers | Zero selects host/cgroup-visible automatic parallelism | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-11 | `page_cache_level` | 0 | Accepted range is 0 through 10; zero disables proactive warmup | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-12 | `target_volume_rows` | 1,048,576 rows | Minimum 65,536 rows; shapes newly produced cold volumes | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-13 | `read_queue_depth` | 1 request | Positive; release keeps portable sequential reads | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |

These byte budgets are not a total RSS cap. Concurrent requests, decoded
values, allocator arenas, thread stacks, metadata and the operating-system page
cache remain separate consumers. Lowering a limit can reject work that would
otherwise fit; raising it requires workload and failure testing.

## Measured scale

The following rows preserve evidence from named experiments. Their source
revisions predate the documentation baseline, so they show achieved scale of
the V6 development line, not a fresh measurement of binary `23bf35df`.

| ID | Observed workload and result | Conditions | Evidence identity |
| --- | --- | --- | --- |
| SCALE-01 | 20,000 rows, 120 tables and 358 indexes; clean-reopen median peak/final RSS was 24,571,904 bytes with the system allocator and 56,848,384 bytes with mimalloc | One storage worker, page-cache level 0, forced database-file eviction; AMD Ryzen 9 7950X, NVMe/Btrfs, Linux | [20k report](../../../evidence/performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.md); engine `1c604d3455056444508ad6fc7de82b3c0a575dd9` |
| SCALE-02 | 100,000,000 rows; active database 1,964,220,021 bytes; three-run verify median peak RSS 547,303,424 bytes and final RSS 293,380,096 bytes | Page-cache level 0; AMD Ryzen 9 7950X, Apacer AS2280Q4U NVMe, Btrfs; one evicted then two cache-hot verify runs | [100M report](../../../evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md); engine `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc` |
| SCALE-03 | 21,600,000 ms workload with 100,000,000 active rows and a 16/32/64/128/256-client ladder; peak server RSS 1,060,020,224 bytes and final RSS 206,327,808 bytes | Intel Celeron 847, 1.76 GiB RAM, 5400 rpm HDD, ext4; side I/O and a real ATA reset; latency is explicitly not an SLA | [Six-hour report](../../../evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md); engine `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |

The benchmarks appendix will preserve full methodology and performance
comparisons when it is published. For SQL support boundaries, use the
[compatibility matrix](../compatibility/).

## Capacity decisions

Before production, test the intended schema and concurrency with the same
allocator, filesystem, storage class and durability settings. Record both
steady state and peaks during import, index creation, compaction, checkpoint,
snapshot and reopen. A successful smaller profile does not validate a larger
profile by extrapolation.
