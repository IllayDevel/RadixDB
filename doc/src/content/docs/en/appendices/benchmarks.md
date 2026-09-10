---
title: Benchmarks
description: Reproducible performance and reliability evidence for the RadixDB 1.2 documentation line.
---

Benchmark results describe one binary, dataset and machine. They are not a
general speed claim, a capacity limit or a service-level objective. This
appendix preserves the source identities, method and conditions behind the
numbers used elsewhere in the manual.

GB below means decimal bytes. MiB and GiB use powers of 1024. RSS is resident
process memory and excludes file data held only by the operating-system cache.

## Evidence map

| ID | Purpose | RadixDB source | Date | Hardware and storage | Primary evidence |
| --- | --- | --- | --- | --- | --- |
| BENCH-01 | Cross-engine 100M performance and footprint | `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc` | 2026-09-06 | AMD Ryzen 9 7950X; Apacer AS2280Q4U 2 TB NVMe; Btrfs; Linux `7.1.3-201.fc44.x86_64` | [100M comparative report](../../../evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md) |
| BENCH-02 | Final 1.1 query-regression gate | `12ef5963` performance code tip | 2026-09-08 | The preserved 100M NVMe/Btrfs database | [1.1 validation report](../../../evidence/performance/RADIXDB_1_1_100M_VALIDATION.md) |
| BENCH-03 | Current 1.2 100M verification | `23bf35df011aae6816d77578be96074b02bc363c` | 2026-09-09 | Canonical NVMe run on device `nvme1n1` | [1.2 validation report](../../../evidence/performance/RADIXDB_1_2_100M_VALIDATION.md) |
| SOAK-01 | Six-hour correctness and recovery under constrained hardware | `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` | 2026-09-07 | Intel Celeron 847, 1.10 GHz; 2 logical CPUs; 1.76 GiB RAM; Toshiba MQ01ABD050 5400 rpm HDD; ext4 | [Six-hour acceptance report](../../../evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md) |

The first two entries are NVMe performance evidence. `SOAK-01` is a reliability
experiment on a degraded HDD path and is never combined with them to calculate
latency or throughput.

## 100M cross-engine method

The trading-style fixture contains exactly 100,000,000 rows and 120 related
tables. Every participant produced checksum `100000000:49734600639880` and the
same query cardinalities.

The PostgreSQL 18.3 baseline was recorded on 2026-08-28 on the same host and
dataset. It completed a fresh schema, load and index lifecycle. The RadixDB
candidate was measured on 2026-09-06. PostgreSQL was not rerun on that date.

For each RadixDB query series, one warm-up was excluded and the median of five
measurements was retained. The table uses the median of three run-level
medians. The first verify run followed targeted `DONTNEED` eviction of database
files; the next two were cache-hot. `page_cache_level` was 0 and storage workers
were automatic.

PostgreSQL COPY used `synchronous_commit=off` with autovacuum disabled. Its load
time is a throughput reference, not an equal-durability comparison with
synchronous RadixDB commits. Database footprint excludes PostgreSQL's larger
cluster and WAL directories.

| ID | Measurement | RadixDB | PostgreSQL 18.3 | Reading |
| --- | --- | ---: | ---: | --- |
| CMP-01 | Logical database | 1,964,220,021 bytes | 16,126,596,799 bytes | RadixDB was 8.21 times smaller for this fixture |
| CMP-02 | Full scan | 100.299 ms | 246.457 ms | RadixDB was 2.46 times faster |
| CMP-03 | Projected scan | 23.189 ms | 84.456 ms | RadixDB was 3.64 times faster |
| CMP-04 | Grouped aggregate with HAVING | 9.907 ms | 24.948 ms | RadixDB was 2.52 times faster |
| CMP-05 | Point lookup | 0.490 ms | 0.237 ms | PostgreSQL was 2.07 times faster |
| CMP-06 | Range lookup | 0.475 ms | 0.269 ms | PostgreSQL was 1.77 times faster |
| CMP-07 | Parent JOIN | 20.790 ms | 12.574 ms | PostgreSQL was 1.65 times faster |
| CMP-08 | UPDATE rollback | 52.568 ms | 13.360 ms | PostgreSQL was 3.93 times faster |
| CMP-09 | DELETE rollback, mixed profile | 3.180 ms | 2.111 ms | PostgreSQL was 1.51 times faster |
| CMP-10 | COPY 100M | 429.087 s | 100.858 s | Throughput reference with different durability settings |
| CMP-11 | CREATE INDEX | 132.690 s | 81.595 s | PostgreSQL was 1.63 times faster |

The result is mixed: RadixDB's scan, projected-scan and aggregate cases were
faster and its database was much smaller; PostgreSQL's lookup, JOIN, rollback,
load and index-build cases in this comparison were faster. No one row is a
summary score for the engines.

## Final 1.1 performance gate

The final release gate did not silently relabel the older numbers. It used
performance source `12ef5963`, the unchanged 100M database, one warm-up and five
measured repeats in each of three predeclared restart-hot runs. Runs affected by
a monotonic OS-cache warm-up trajectory were discarded before comparison.

| ID | Case | 1.1 candidate median | Accepted baseline | Delta |
| --- | --- | ---: | ---: | ---: |
| PERF-01 | Aggregate | 10.150 ms | 9.907 ms | +2.45% |
| PERF-02 | Checksum | 12.007 ms | 11.638 ms | +3.17% |
| PERF-03 | Fact dictionary | 572.626 ms | 592.686 ms | -3.38% |
| PERF-04 | Full scan | 105.224 ms | 100.299 ms | +4.91% |
| PERF-05 | UPDATE rollback | 42.994 ms | 52.568 ms | -18.21% |
| PERF-06 | DELETE rollback | 2.285 ms | 3.180 ms | -28.13% |

The worst listed regression, 4.91%, remained below the predeclared 1.20-times
corridor. All three runs preserved 100,000,000 rows, checksum
`49734600639880` and access-path digest
`499488e7eeca18a91a2ebb763473308e15d4e929a7c0770d933c9fd9bba084d4`.
Peak RSS was 563,662,848, 566,497,280 and 551,399,424 bytes. These measurements
belong to `12ef5963`; later correctness hardening is not presented as the same
benchmark binary.

## Current 1.2 verification

The current 1.2 source baseline reran the canonical 100-million-row NVMe check.
It preserved checksum `100000000:49734600639880`, reported no storage or
resource errors, and completed the measured run in 11,870.421 ms. The cold
database-selection phase took 523.413 ms. Peak RSS was 565.97 MiB, final RSS
was 343.04 MiB, and the allocated database size was 1.83 GiB.

This run verifies the current source after the 1.2 server, security and
reliability changes. It is not substituted into the earlier PostgreSQL table:
that comparison belongs to a different RadixDB binary and measurement date.

## Six-hour reliability run

`SOAK-01` used 100,000,000 active rows and a client ladder of 16, 32, 64, 128
and 256. The workload phase lasted 21,600,000 ms; the complete run including
seed and terminal evidence lasted 27,427,411 ms.

| ID | Observation | Result |
| --- | --- | ---: |
| SOAK-02 | Operations | 2,351,035 |
| SOAK-03 | Committed transactions | 98,668 |
| SOAK-04 | Invariant checks / failures | 2,100 / 0 |
| SOAK-05 | Graceful and SIGKILL reopens | 2 |
| SOAK-06 | Peak server RSS | 1,060,020,224 bytes |
| SOAK-07 | Final server RSS | 206,327,808 bytes |

The run included competing disk writes and a real ATA bus error during
`FLUSH CACHE EXT`. Linux reset the SATA link and retried the flush. Progress
resumed, and final checkpoint, snapshot, restore and logical-digest comparison
passed. This demonstrates the observed recovery path. It does not prove
recovery from permanent device loss, false flush acknowledgement or arbitrary
power loss.

The HDD was intentionally a constrained and degraded environment. Its latency
and throughput are not a product SLA and must not be averaged with the NVMe
results.

## Reproducing a result

A new published benchmark must record the source SHA, lockfile and binary
digests, build profile, allocator, operating system, CPU, memory, exact storage
device and filesystem. It must also preserve dataset checksum, participant
configuration, cache state, warm-up policy, sample count, aggregation method
and raw result locations.

Run all participants sequentially in an isolated benchmark root. Never point a
benchmark harness at a production database. A rerun with different durability,
cache state or hardware is a new result, not another sample of the old one.

See [limits](../limits/) for measured memory and footprint profiles and
[release notes](../release-notes/) for current and historical release boundaries.
The [evidence archive](../evidence/) lists every compact report and supporting
file distributed with this manual.
