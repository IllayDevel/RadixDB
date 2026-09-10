# CA-80.5c — micro-device 20k footprint profile

[Русский](CA_80_5C_MICRO_DEVICE_20K_REPORT.ru.md)

Date: 2026-09-06.

## Verdict

The `20k` profile is accepted as a reproducible lower Linux resource boundary
for the micro-device/minimum-footprint scenario. It does not replace the
canonical embedded, low-end and constrained-hardware profile, which remains
`10M`. The profiles are deliberately separate:

- `10M` demonstrates practical operation of a large database on weak hardware;
- `20k` measures the minimum process cost, a wide schema, catalog/artifact
  metadata, and basic DDL/DML/SELECT/recovery functionality.

The production default remains `mimalloc`. In the constrained fresh run it was
`4.5%` faster than the system allocator, had `15.1%` lower final RSS and built
indexes `17.3%` faster. For the smallest clean runtime, the system allocator is
retained as an explicit build policy: its clean-reopen median is `23.4 MiB`,
versus `54.2 MiB` for `mimalloc`.

Release packaging now separates DWARF into a diagnostic artifact. The installed
`radixdb-server` size fell from `74.05 MiB` to `15.56 MiB`, but this is not
presented as a runtime RSS reduction: machine code, data and every ELF
`PT_LOAD` segment are identical between the original and compact binaries.

## Candidate and reproducibility

- benchmark source: `1c604d3455056444508ad6fc7de82b3c0a575dd9`;
- split-debug packaging source:
  `7ccc71c637a9a1c25f4955e4643107acdb5ef1a4`;
- version/protocol: `0.5.1` / `13`;
- Cargo.lock SHA-256:
  `a0dfe4bc1679224fadc97f983574a92e50d326c4f3c3ad44d1c9ce550e1dcfbc`;
- host: AMD Ryzen 9 7950X, 32 logical CPUs, Linux
  `7.1.3-201.fc44.x86_64`, NVMe/Btrfs;
- workload: `120` tables, exactly `20 000` rows and `358`
  secondary/constraint indexes;
- constrained configuration: one storage worker, page-cache level `0`, and
  prefetch/block-cache budgets of `0` in the benchmark harness;
- a fresh run followed by three alternating system/mimalloc clean reopen runs
  with targeted database page-cache eviction;
- explicit bounded page-cache publication followed by another reopen.

Saved benchmark binaries:

| Allocator | `radixdb-bench` SHA-256 |
|---|---|
| system | `257897b343562f925735c4f43da2eaf057aa00b3e58fdbd05e8fcf721a2e3f5a` |
| mimalloc | `cb7311f7dbc30be9b45112f619a88d6d9716419c02076dc3e748ba8b2c1ac3a3` |

## Fresh 20k matrix

A negative delta means an improvement for `mimalloc` over the system
allocator. `participant.run` includes creation of `120` tables, loading,
indexes and the query profile.

| Metric | System | mimalloc | Delta |
|---|---:|---:|---:|
| full participant elapsed | 19,663.122 ms | 18,786.498 ms | -4.5% |
| seed COPY | 313.793 ms | 367.897 ms | +17.2% |
| index build | 5,883.642 ms | 4,867.365 ms | -17.3% |
| peak RSS | 59,588,608 B | 64,684,032 B | +8.6% |
| final RSS | 55,570,432 B | 47,169,536 B | -15.1% |
| final logical storage during run | 9,391,756 B | 9,393,967 B | +0.02% |
| final allocated storage during run | 11,251,712 B | 11,251,712 B | 0% |

Both runs confirmed checksum `20000:72539880`. The small seed consists of
`120` short COPY transactions, so the `54.1 ms` difference must not be
extrapolated to bulk-load throughput for larger profiles.

### Why the first `mimalloc` run occupied about 120 MB

The initial diagnostic run used `storage_cpu_workers=auto` and reached `39`
threads. Peak/final RSS was `137,891,840 / 120,180,736 B`, or
`131.5 / 114.6 MiB`. In the accepted minimum-footprint profile with one storage
worker, the corresponding values were `61.7 / 45.0 MiB`, a reduction of about
`69.8 / 69.6 MiB`.

The principal measured difference in this comparison is the cost of the
parallel runtime, thread stacks and allocator arenas. The exact A/B below did
not reproduce a difference of tens of megabytes between unstripped and
split-debug forms of one binary. This is not a universal claim that debug
layout can never affect mappings, faults or loader/tooling behavior; the result
is limited to this ELF, host and workload. `workers=auto` remains the correct
throughput default, while `workers=1` is a deliberate minimum-RSS profile for
a constrained device.

## Clean-reopen median

Each value is the median of three alternating forced-eviction runs on the same
created generation.

| Metric | System | mimalloc | Delta |
|---|---:|---:|---:|
| verify elapsed | 268.337 ms | 249.994 ms | -6.8% |
| cold start + select database | 67.561 ms | 50.441 ms | -25.3% |
| peak/final RSS | 24 571 904 B | 56 848 384 B | +131.4% |
| checksum | 12.086 ms | 12.069 ms | -0.1% |
| PK lookup | 0.082 ms | 0.079 ms | -4.3% |
| range | 0.059 ms | 0.059 ms | -0.6% |
| full scan | 0.198 ms | 0.209 ms | +5.7% |
| UPDATE rollback | 8.360 ms | 2.544 ms | -69.6% |
| DELETE rollback | 2.561 ms | 1.856 ms | -27.6% |

These are two different lifecycle views. Fresh final RSS measures the tail
immediately after schema creation, COPY and index construction; clean reopen
measures the base cost of an already-created database. The values must not be
added together or used interchangeably.

## Physical footprint

After an explicit checkpoint and subsequent reopen, the complete database root
occupies `4,509,673 B` logical and `6,422,528 B` allocated. Breakdown:

| Artifact | Files | Logical bytes |
|---|---:|---:|
| `.data` | 120 | 1,321,272 |
| `.idx` | 120 | 224,312 |
| `.cat` | 2 | 1,364,144 |
| `.mft` | 366 | 1,591,584 |
| WAL `.log` | 2 | 162 |
| service files | 3 | 8,199 |

This fixture is deliberately metadata-heavy: only `20k` rows are distributed
across `120` tables. The result is therefore a wide-schema boundary, not the
minimum footprint of one table.

## Checkpoint and reopen

The separate `checkpoint-mimalloc` run forced publication of a bounded
page-cache generation (`state=complete`, target/warmed `1 B`) and preserved
checksum `20000:72539880`. The subsequent forced-eviction
`reopen-after-checkpoint-mimalloc` run confirmed the same checksum and completed
the full SELECT/rollback profile. Peak/final RSS was `60,489,728 B`.

## Compact release executable

The canonical release retains `line-tables-only` in the build artifact. Then
`package-artifacts.sh` creates a compact installed binary, detached
`debug/radixdb-server.debug`, and `.gnu_debuglink`, and verifies a shared ELF
Build ID.

| Metric | Unstripped | Installed split-debug | Delta |
|---|---:|---:|---:|
| file size | 77,650,160 B | 16,317,472 B | -79.0% |
| `text` | 13,677,059 B | 13,677,059 B | 0% |
| `data` | 365,296 B | 365,296 B | 0% |
| `bss` | 43,009 B | 43,009 B | 0% |
| total `text+data+bss` | 14,085,364 B | 14,085,364 B | 0% |
| detached debug artifact | — | 63,581,840 B | — |

All four `PT_LOAD` segments have identical file/memory sizes and flags. Three
alternating starts of an empty constrained server produced median idle RSS of
`6,868 KiB` unstripped and `6,740 KiB` split-debug, with identical `VmSize` of
`1,066,264 KiB`. The `128 KiB` RSS difference is start-up noise; this idle view
did not reveal a difference of tens of runtime megabytes.

### Exact split-debug benchmark A/B

After the initial report, an additional fresh `20k` A/B was performed on the
same production-allocator benchmark ELF. `objcopy --strip-debug` and
`.gnu_debuglink` preserved one Build ID and byte-identical `PT_LOAD` segments.
Three consecutive alternating runs on separate roots produced:

| Metric, median of 3 runs | Unstripped | Split-debug | Split delta |
|---|---:|---:|---:|
| participant elapsed | 18,651.119 ms | 18,633.892 ms | -0.09% |
| peak RSS | 80,990,208 B | 84,434,944 B | +4.25% |
| final RSS | 60,997,632 B | 61,554,688 B | +0.91% |
| minor faults | 26,599 | 23,802 | -10.52% |
| seed COPY | 359.825 ms | 361.878 ms | +0.57% |
| index build | 4,907.025 ms | 4,862.601 ms | -0.91% |

RSS and elapsed ranges overlap; this profile shows no sustained runtime RSS
gain from split-debug. The check does not exclude an effect from debug layout
in another environment, but it does not allow the earlier reduction of about
`69.6 MiB` to be attributed to removing DWARF. The exact cause remains
`workers=auto -> workers=1`.

The benchmark ELF size fell from `95,964,736` to `19,851,952 B`; detached debug
occupies `78,639,136 B`. All six runs confirmed checksum `20000:72539880`.

Exact packaged server:

- installed SHA-256:
  `b2e0043c8b6f90d0ef2209db8ee1d4993b1fa4ac8d888674d3debcb8762e86f8`;
- detached debug SHA-256:
  `209c49a8dc386eb72e3069a6c8a7c6b7d501454f42680db92ed449ee875acc43`;
- bundle provenance: clean commit `7ccc71c637a9a1c25f4955e4643107acdb5ef1a4`;
- the complete release bundle, checksum inventory, systemd dry
  install/uninstall, external backup/restore and tamper rejection passed.

## Evidence

Raw benchmark results and saved binaries are stored outside the public Git
repository. Their local paths are intentionally omitted from the published
copy. The SHA-256 values below identify the original `REPORT.md` files when
they are transferred separately.

Fresh `REPORT.md` SHA-256:

- system: `f9341719e9650394af9feb3a5974986c74dda3c02ebdc2d4ea68c0d2c9f8dda4`;
- mimalloc: `51dc19c1a20fe4b9cb80a6fc13cc221c983b7dde0b17be6abc425934849b1eef`.
