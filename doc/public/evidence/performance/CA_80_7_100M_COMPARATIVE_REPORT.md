# CA-80.7 — comparative 100M NVMe benchmark

[Русский](CA_80_7_100M_COMPARATIVE_REPORT.ru.md)

Date: 2026-09-06.

## Decision

CA-80.7 is accepted. The candidate preserved the checksum and matching
cardinalities, kept the cost of later seed chunks bounded, and stayed within
the mandatory `<=1.20x` corridor against the previous RadixDB for every
comparable timing except `delete.rollback` in the full mixed profile. This one
deviation was accepted as an explicit exception: an isolated DELETE run
measured `2.267 ms`. The `20%` rule is unchanged; the exception does not apply
to other metrics or future runs.

The main physical result of the new storage generation is a `26.3%` smaller
active database, `93.8%` lower fresh final RSS and `68.5%` lower verification
peak RSS. The fresh lifecycle slowed by `8.8%` and index construction by
`15.2%`; both remain inside the accepted corridor.

## Identity and method

- candidate source: `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc`;
- Cargo.lock SHA-256:
  `a0dfe4bc1679224fadc97f983574a92e50d326c4f3c3ad44d1c9ce550e1dcfbc`;
- exact release `radixdb-bench` SHA-256:
  `1b2500760e957dbc0bbce94b3d6be6bf19f4e6c7599d2518b46bb4eef4684d5d`;
- build: release, `bench-harness`, production-default `mimalloc`;
- host: AMD Ryzen 9 7950X, Apacer AS2280Q4U 2 TB NVMe, Btrfs,
  Linux `7.1.3-201.fc44.x86_64`;
- dataset: unchanged 100M fixture, checksum
  `100000000:49734600639880`, page-cache level `0`, storage workers `auto`;
- fresh previous-RadixDB evidence: revision
  `aada83a4a46cdf80caa541e4bc0acc7c21a26bf1`;
- canonical previous-RadixDB verification baseline: revision
  `cce945773038e150845760e82327d81c5cdb2eae`,
  [`FINAL_100M_NVME_ACCEPTANCE_REPORT.md`](FINAL_100M_NVME_ACCEPTANCE_REPORT.md);
- frozen old-engine index baseline: `115,158.500 ms`, recorded before the
  candidate run in CA-80.5;
- frozen PostgreSQL baseline:
  [`icp621-pg18-page-cache-matrix-100m-20260828.md`](icp621-pg18-page-cache-matrix-100m-20260828.md),
  PostgreSQL `18.3`, on the same host and dataset.

PostgreSQL was not rerun on 2026-09-06; the preserved complete fresh run from
2026-08-28 is used. Its COPY used `synchronous_commit=off` with autovacuum
disabled, so import is a throughput reference rather than a durability-equivalent
comparison with synchronous RadixDB commits.

The candidate fresh run was performed once. Verify-existing was performed
three times with the same binary: run 1 after targeted `DONTNEED` eviction of
database files, and runs 2/3 without eviction. In each query series one warm-up
was excluded and the median of five measurements was taken. The summary
latency below is the median of three run-level medians. Cold-start and resource
elapsed values are shown separately, so cold and warm states are not hidden in
one average.

## Fresh lifecycle

| Phase | Candidate | Previous RadixDB | Delta | PostgreSQL 18.3 | Candidate / PG |
|---|---:|---:|---:|---:|---:|
| full participant | `572.329 s` | `525.955 s` | `+8.817%` | `182.558 s` | `3.135x` |
| COPY 100M | `429.087 s` | `393.930 s` | `+8.925%` | `100.858 s` | `4.254x` |
| CREATE INDEX | `132.690 s` | `115.159 s` frozen | `+15.224%` | `81.595 s` | `1.626x` |

Candidate COPY throughput was `233,053 rows/s`. All `120` chunks have a fixed
cardinality of approximately `833,333`; the median over all chunks was
`3,483.246 ms`, the steady window 13..108 was `3,484.360 ms`, and the final 12
were `3,477.425 ms`. The late/steady ratio was `0.998x`, and the
maximum/overall-median ratio was `1.683x`: chunk cost did not grow with the
amount of data already loaded.

## Footprint, memory and writes

| Metric | Candidate | Previous RadixDB | Delta |
|---|---:|---:|---:|
| fresh peak RSS | `2,176,487,424 B` | `36,376,854,528 B` | `-94.0%` |
| fresh final RSS | `1,241,767,936 B` | `19,942,359,040 B` | `-93.8%` |
| process write bytes | `30,400,126,976 B` | `25,533,755,392 B` | `+19.1%` |
| device write bytes | `28,044,476,416 B` | `26,062,774,272 B` | `+7.6%` |
| logical database | `1,964,220,021 B` | `2,666,165,781 B` | `-26.3%` |
| allocated database | `1,965,780,992 B` | `2,667,888,640 B` | `-26.3%` |
| files / directories | `454 / 148` | `364 / 125` | bounded artifact layout |

The PostgreSQL database occupies `16,126,596,799 B`, or `8.21x` the candidate;
the complete cluster including WAL occupies `32,885,882,880 B`, or `16.74x`
the candidate. These are storage-footprint values, not physical-write-byte
measurements.

## Verify-existing and resources

| Run | Initial cache state | Elapsed | Peak RSS | Final RSS | Process read |
|---|---|---:|---:|---:|---:|
| 1 | database files evicted | `9.266 s` | `547,303,424 B` | `311,193,600 B` | `116,617,216 B` |
| 2 | cache-hot | `8.837 s` | `538,542,080 B` | `293,380,096 B` | `0 B` |
| 3 | cache-hot | `8.947 s` | `611,880,960 B` | `283,430,912 B` | `0 B` |
| median | deliberately mixed, summary only | `8.947 s` | `547,303,424 B` | `293,380,096 B` | — |

The previous canonical verification measured `14.274 s`, peak RSS
`1,738,457,088 B`, and an active database of `2,666,165,783 B`. The candidate
was therefore `37.3%` faster, used `68.5%` less peak RSS and stored `26.3%`
less data. All three runs passed the checksum, cardinality and rollback oracle.
The candidate canonicalized access-path SHA-256 was identical:
`da6d4c0bfc27131ce3c2403791e28f3050c5ca7187e4981f10531cb797bdfccd`.

## Query latency

A negative delta means an improvement. `Candidate / PG < 1` means the
candidate is faster than PostgreSQL. PostgreSQL has no navigation syntax; the
corresponding rows use the classic JOIN analogue described in the PostgreSQL
baseline.

| Case | Candidate, ms | Previous RDB, ms | Delta | PG, ms | Candidate / PG |
|---|---:|---:|---:|---:|---:|
| `correctness.seed_checksum` | `11.638` | `12.497` | `-6.9%` | `5823.818` | `0.002x` |
| `select.pk` | `0.490` | `0.481` | `+1.9%` | `0.237` | `2.068x` |
| `select.range` | `0.475` | `0.485` | `-2.0%` | `0.269` | `1.768x` |
| `scan.full` | `100.299` | `93.247` | `+7.6%` | `246.457` | `0.407x` |
| `scan.projected` | `23.189` | `21.055` | `+10.1%` | `84.456` | `0.275x` |
| `aggregate.group_having` | `9.907` | `10.286` | `-3.7%` | `24.948` | `0.397x` |
| `join.parent` | `20.790` | `17.945` | `+15.9%` | `12.574` | `1.653x` |
| `reference.navigation` | `31.972` | `27.313` | `+17.1%` | `9.283` analogue | `3.444x` |
| `reference.direct.explicit` | `20.190` | `18.233` | `+10.7%` | `8.918` | `2.264x` |
| `reference.fact_dictionary` | `592.686` | `725.155` | `-18.3%` | `344.862` analogue | `1.719x` |
| `reference.fact_first` | `376.190` | `415.987` | `-9.6%` | `358.814` | `1.048x` |
| `reference.target_first` | `354.408` | `378.050` | `-6.3%` | `379.936` | `0.933x` |
| `update.rollback` | `52.568` | `44.532` | `+18.0%` | `13.360` | `3.935x` |
| `delete.rollback` | `3.180` | `1.746` | **`+82.1%`** | `2.111` | `1.506x` |

### Accepted DELETE exception

The full profile executes UPDATE and DELETE sequentially and shows a
consistently more expensive DELETE than the previous revision. To isolate it
from the adjacent operation, the exact candidate was run separately: after one
warm-up, five samples had a median of `2.267 ms` (`2.216..2.284 ms`). This is
`+7.4%` versus PostgreSQL at `2.111 ms`, but `+29.8%` versus previous RadixDB at
`1.746 ms`.

An earlier separate 20k micro-reference (`0.155 ms` for RadixDB versus the
reported `2.3 ms` for PostgreSQL, about `14.8x` faster) is not used in the 100M
verdict because the scale and cardinality differ.

## Raw evidence

- fresh: `RadixTest/results/ca80-7-final-100m-b648b2d3-20260906`;
- verification: `ca80-7-verify-100m-b648b2d3-r{1,2,3}-20260906`;
- isolated DELETE: `ca80-7-delete-isolated-100m-b648b2d3-20260906`;
- previous fresh: `RadixTest/results/icp7-current-100m-fresh-aada83a`;
- PostgreSQL: `RadixTest/results/icp621-matrix-pg18-100m-92b1dcf`.

SHA-256 `results.json / REPORT.md`:

- fresh: `ad6b654ab342107299ce22cccb12106e6d2b9034419d80df2bc00b8ee5f5f57c` /
  `b8b4b5f59d10c2d916eb11af1381aa652f76b9aac8c7ab952b43dec984cb44b9`;
- verification r1: `928fbdf59752baea11e9465da0e08aa4ba11e482a9c5d22e2e9ab83b78570f6e` /
  `e4df474a132f5c4da0ca280736607f19857aa4c39deabe6fdf5a41848da7a1f4`;
- verification r2: `079d78da310f3c9ac650e6a9d3ad876cf96325ee9411b81937b17a0bf2016613` /
  `34ed4e50e64ed6ab256a24e33e0209c861f71f975729107c13aa3043088ea929`;
- verification r3: `fb04b56e5339640788ce0db9cad5fd34f8c4ca17cda1e3042829244dc7d1a8c6` /
  `6a3f4d37bb5ae03a8094af5d1ce3cf1e4853e10aa692546cf5a9c4a1a78dc422`;
- isolated DELETE: `0275449addd99000df324615ec930af1fc15044de191194f77d59b82b89a51c4` /
  `6b3e8df20667ed19ebc36da78e483966a5f56e35ccd77c53c72725f1b1635ca9`.

Raw artifacts are stored outside Git. This compact, reproducible identity
record remains in Git.
