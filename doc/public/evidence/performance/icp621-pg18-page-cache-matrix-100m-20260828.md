# ICP-6.21 — PostgreSQL 18 / page-cache matrix 100M

[Русский](icp621-pg18-page-cache-matrix-100m-20260828.ru.md)

Date: 2026-08-28.

## Decision

The comparative 100M gate passed. PostgreSQL 18.3 recreated the schema, loaded
100M rows and built the indexes. Current RadixDB then opened the same
current-format 100M fixture four times with `page_cache_level = 0/1/5/10`.
Before every RadixDB run, the harness applied Linux `DONTNEED` only to the
benchmark database files and then explicitly waited for warm-up completion.

All five participants produced checksum `100000000:49734600639880`, matching
query cardinalities and matching rollback results. The RadixDB storage
footprint did not change across cache levels. The production default remains
`0`.

## Identity and method

- Tree HEAD during the matrix: `92b1dcf`; production engine/binary source:
  `e27d98e21f04b3a46136fc02c36f8f508a9f9c34`. The following two commits only
  changed the acceptance test and documentation.
- Release binary SHA-256:
  `6d73eec7f1423d3e9c45023bbea9ee5f290c48f19ba3773f5f72c4c5a8778377`.
- Cargo.lock SHA-256:
  `84a717843ff6e896245d0f46f63c361ba0c3816c89d38b1aef41999f8fac7737`.
- PostgreSQL `18.3`, dedicated ownership-verified cluster, `nvme-local`
  profile: `shared_buffers=4GB`, `effective_cache_size=24GB`,
  `maintenance_work_mem=2GB`, `work_mem=256MB`, parallel gather `4`, JIT off.
- The PostgreSQL bulk profile used `synchronous_commit=off` and autovacuum off;
  this is an explicit throughput contract. Its import must not be presented as
  durability-equivalent to synchronous RadixDB commits.
- PostgreSQL ran a fresh lifecycle. RadixDB used one unchanged existing-data
  fixture, intentionally isolating page-cache policy from four repeated imports.
- RadixDB UPDATE/DELETE values are the median of `31` measurements after `5`
  warm-ups. The full PostgreSQL lifecycle contains one DML measurement and is
  shown as an external reference.
- `% PG` is `RadixDB time / PostgreSQL time`; values below `100%` mean RadixDB
  was faster. `Delta 10/0` compares full warm-up with cache level `0`.

## Full comparison matrix

| Case | PG, ms | RDB L0, ms | L0 / PG | RDB L1, ms | L1 / PG | RDB L5, ms | L5 / PG | RDB L10, ms | L10 / PG | Delta 10/0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `correctness.seed_checksum` | 5823.818 | 13.419 | 0.2% | 14.996 | 0.3% | 13.994 | 0.2% | 12.849 | 0.2% | -4.2% |
| `select.pk` | 0.237 | 0.480 | 202.3% | 0.482 | 203.0% | 0.486 | 205.0% | 0.489 | 206.3% | +2.0% |
| `select.range` | 0.269 | 0.476 | 177.3% | 0.498 | 185.3% | 0.475 | 176.7% | 0.470 | 175.0% | -1.3% |
| `scan.full` | 246.457 | 97.175 | 39.4% | 93.499 | 37.9% | 97.501 | 39.6% | 88.522 | 35.9% | **-8.9%** |
| `scan.projected` | 84.456 | 20.795 | 24.6% | 20.656 | 24.5% | 21.272 | 25.2% | 20.661 | 24.5% | -0.6% |
| `aggregate.group_having` | 24.948 | 10.379 | 41.6% | 10.411 | 41.7% | 10.655 | 42.7% | 10.525 | 42.2% | +1.4% |
| `join.parent` | 12.574 | 20.001 | 159.1% | 19.489 | 155.0% | 20.206 | 160.7% | 19.978 | 158.9% | -0.1% |
| `reference.navigation` | 9.283 | 28.123 | 303.0% | 28.705 | 309.2% | 30.360 | 327.1% | 28.347 | 305.4% | +0.8% |
| `reference.direct.explicit` | 8.918 | 187.332 | 2100.6% | 181.239 | 2032.3% | 183.705 | 2059.9% | 184.753 | 2071.7% | -1.4% |
| `reference.fact_dictionary` | 344.862 | 544.913 | 158.0% | 531.400 | 154.1% | 557.919 | 161.8% | 539.360 | 156.4% | -1.0% |
| `reference.fact_first` | 358.814 | 732.736 | 204.2% | 730.196 | 203.5% | 752.727 | 209.8% | 725.919 | 202.3% | -0.9% |
| `reference.target_first` | 379.936 | 786.354 | 207.0% | 801.102 | 210.9% | 801.850 | 211.0% | 785.340 | 206.7% | -0.1% |
| `update.rollback` | 13.360 | 39.962 | 299.1% | 36.331 | 271.9% | 36.219 | 271.1% | 35.665 | 267.0% | **-10.8%** |
| `delete.rollback` | 2.111 | 1.608 | 76.2% | 1.639 | 77.6% | 1.599 | 75.8% | 1.626 | 77.1% | +1.1% |

PostgreSQL does not support the RadixDB navigation syntax. In the
`reference.navigation` and `reference.fact_dictionary` rows, PostgreSQL runs
the corresponding classic JOIN shape; matching PostgreSQL rows and checksums
serve as the semantic reference. Within RadixDB, navigation at level `10` is
`6.52x` faster than its own explicit LEFT JOIN (`28.347` versus `184.753 ms`).

## RadixDB warm-up and resources

| Level | Target/read/resident | Warm-up wall time | Read rate | Peak RSS | Peak FD | Storage allocated |
|---:|---:|---:|---:|---:|---:|---:|
| `0` | `0 B` | `0.129 ms` | `0 B/s` | `2204.1 MiB` | `134` | `2,667,393,024 B` |
| `1` | `266,605,528 B` | `104.258 ms` | `2.34 GiB/s` | `2206.6 MiB` | `134` | `2,667,393,024 B` |
| `5` | `1,333,027,640 B` | `502.523 ms` | `2.46 GiB/s` | `2185.3 MiB` | `135` | `2,667,393,024 B` |
| `10` | `2,666,055,280 B` | `1022.348 ms` | `2.42 GiB/s` | `2186.1 MiB` | `135` | `2,667,393,024 B` |

Warm-up target, bytes read and resident estimate matched at levels `1/5/10`.
Full warm-up did not increase process RSS: the data belongs to the operating
system page cache, not the engine heap. Level `10` consistently improved the
full-scan and UPDATE medians, but did not accelerate CPU/planner-bound point,
JOIN or navigation cases; small changes there remain within run-to-run noise.

## Lifecycle and footprint

PostgreSQL fresh lifecycle:

| Phase | Time |
|---|---:|
| schema create | `105.678 ms` |
| COPY 100M | `100.858 s` |
| CREATE INDEX (358) | `81.595 s` |
| create + COPY + indexes | `182.558 s` |

Footprint after loading:

| Engine/path | Bytes | GiB | PG / RadixDB |
|---|---:|---:|---:|
| PostgreSQL database (`pg_database_size`) | `16,126,596,799` | `15.02` | `6.05x` |
| PostgreSQL entire data directory, including WAL | `32,885,882,880` | `30.63` | `12.33x` |
| RadixDB server data | `2,667,393,024` | `2.48` | `1.00x` |

After the fresh PostgreSQL load, about `15.58 GiB` was stored separately in
`pg_wal`; database-only and complete-cluster ratios are therefore shown
separately.

## Raw artifacts

- PostgreSQL: `RadixTest/results/icp621-matrix-pg18-100m-92b1dcf`;
- full RadixDB matrix:
  `icp621-matrix-radix-l{0,1,5,10}-100m-92b1dcf`;
- isolated RadixDB DML:
  `icp621-matrix-radix-l{0,1,5,10}-{update,delete}-rollback-repeat-100m-92b1dcf`.

The local PostgreSQL instance was stopped after artifact collection. The
production RadixDB process under `/opt/radixdb` was not touched.
