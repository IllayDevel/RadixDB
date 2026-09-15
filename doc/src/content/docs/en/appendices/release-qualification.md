---
title: Release Qualification
description: Why RadixDB 1.2.25 is classified as stable, which evidence supports that status and what it does not cover.
---

RadixDB 1.2.25 is the current **stable packaged release**. In this manual,
stable means that a fixed release candidate passed the project's required
correctness, recovery, compatibility, documentation and packaging gates. It
does not mean that RadixDB has the deployment history, ecosystem or operational
breadth of a database developed in public for decades.

Version 1.2.25 is the first publicly packaged release, not the first tested
RadixDB build. The engine was developed and qualified before the public GitHub
repository and binary distribution were created. Release status is therefore
based on recorded qualification results rather than the age of the public
repository.

## Release identity

| Property | Qualified value |
| --- | --- |
| Release | `1.2.25` |
| Annotated tag | [`v1.2.25`](https://github.com/IllayDevel/RadixDB/releases/tag/v1.2.25) |
| Source commit | `63f45972475dd14825690009ee9e10e97707a3c7` |
| Wire protocol | `18` |
| Rust toolchain | `1.97.0` |
| Required tag CI | [Completed successfully](https://github.com/IllayDevel/RadixDB/actions/runs/34845870019) on the exact source commit |
| Packaged target | Linux x86-64 GNU; the 1.2.25 archive requires glibc 2.38 or newer |

The annotated tag and published archives are immutable release identities.
Later documentation revisions can clarify the contract but do not change the
binary that was qualified.

## Automated release gates

The release CI runs the complete default workspace inventory with all Cargo
targets. Enumeration of that release source contains **6,949 named correctness
tests**: 3,100 unit tests and 3,849 integration or regression tests. The same
inventory also exposes 54 Criterion benchmark cases, which are counted
separately and are not presented as correctness tests.

The count can be reproduced without executing the tests:

```bash
cargo test --locked --workspace --all-targets -- --list --format terse
```

The required CI executes, rather than only enumerates, the default inventory:

```bash
cargo test --locked --workspace --all-targets --no-fail-fast
```

The tag gate also includes:

- executable doctests and a test that owns the public API documentation
  contract;
- formatting, strict Clippy and immutable `Cargo.lock` checks;
- SQL type, partial-index TCP, restore, `COPY FROM`, client and public-build
  contract tests;
- differential SQLite oracles, failpoint I/O, file-backed execution,
  no-default-feature and optional-feature checks;
- Linux AArch64 cross-compilation plus selected SIMD and extension ABI tests
  under QEMU;
- revision-bound coverage floors of 75% for lines, functions and regions, and
  55% for branches. The release recorded 77.32%, 75.11%, 77.51% and 61.79%
  respectively;
- release binary construction, lifecycle checks, server/client smoke tests and
  offline public examples;
- the bilingual Astro manual, content parity, links, publication packaging and
  desktop/mobile browser scenarios.

The [successful tag run](https://github.com/IllayDevel/RadixDB/actions/runs/34845870019)
is the authoritative result for these automated gates.

## Recovery and endurance evidence

Large qualification runs are preserved with their own binary and source
identities. They establish accepted behavior of the V6 and 1.2 engine line;
they are not relabeled as fresh executions of the final 1.2.25 tag.

- The [1.2 validation](../../../evidence/performance/RADIXDB_1_2_100M_VALIDATION.md)
  at source `23bf35df011aae6816d77578be96074b02bc363c` reopened and verified
  the canonical 100-million-row database, preserving checksum
  `100000000:49734600639880` with no storage or resource errors.
- The [six-hour endurance report](../../../evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md)
  for engine/soak source `dd0bf75c9176bceb70ce8f1d2a07057610ec381b`
  records 2,351,035 operations, a ladder up to 256 clients and 2,100 successful
  invariant checks with zero failures on a 1.76 GiB host.
- That endurance run exercised graceful reopen, process-kill reopen,
  checkpoints, snapshot publication, restore into a separate database and an
  equal source/restore logical digest
  `59b56e6b7bdaf846dd167aa01185222c7af95aec54593b9a05927b4c0abda4b0`.
- During the run, a real transient SATA transport failure interrupted a flush.
  The kernel restored the link, the engine resumed progress and the final
  recovery oracle passed. This is evidence for that observed transient fault,
  not a claim of survival after permanent media loss.
- Release CI keeps recovery behavior under regression coverage through WAL,
  checkpoint, corruption, snapshot, restore and process-abort test families.
  Backup and restore procedures are described separately in
  [Backup and restore](../../administration/backup-restore/).

The [public evidence archive](../evidence/) retains the accepted reports,
hardware conditions, exact revisions, checksums and known slower cases.

## Compatibility and format qualification

Stability applies to the documented SQL, protocol 18, configuration and V6
storage boundary. Tests cover same-format reopen, WAL replay, catalog
publication, checkpoints, snapshots, restore and selected fail-closed format
transitions. The [compatibility matrix](../compatibility/) is the authority for
supported SQL and interfaces.

Stability does not promise that arbitrary older physical database layouts can
be opened or upgraded in place. Unsupported physical generations fail closed;
cross-boundary migration uses the separately verified logical export/import
procedure in [Upgrading RadixDB](../../administration/upgrading/).

## Additional development controls

The public Nightly workflow defines rotating mutation-testing shards, Miri
groups, stress tests and sanitizer jobs. These are continuing development
controls. A complete successful all-shard Nightly run is not attached to the
1.2.25 tag, so mutation testing and the other Nightly groups are not counted as
release-specific evidence for its stable status.

This distinction keeps the release claim reproducible: required tag gates are
identified by one successful run, while ongoing or partially rotated analysis
is not silently promoted into release acceptance.

## What stable does not cover

The 1.2.25 stable classification does not claim:

- a long independent production history, a broad integration ecosystem or a
  mature market of third-party operational support;
- built-in replication, automatic failover, high availability, arbitrary-WAL-
  position PITR, distributed writes or automatic sharding;
- serializable isolation or PostgreSQL wire compatibility;
- survival after permanent disk loss, false flush completion or every possible
  power-loss and filesystem failure mode;
- a 24-hour or 72-hour endurance result, a throughput SLA or universal
  performance superiority;
- a supported packaged binary outside Linux x86-64 GNU. AArch64 CI coverage is
  a portability check, not a packaged-platform commitment;
- compatibility with glibc older than 2.38 for the 1.2.25 binary archive;
- automatic validation of an application's schema, workload, backup schedule
  or recovery objectives.

Before production use, run the application's own acceptance workload and
restore rehearsal on the intended hardware and filesystem. Consult
[Choosing RadixDB](../comparison/) for workload fit and [Limits](../limits/)
for structural and configurable boundaries.

## Why the first public release can be stable

> Release status is determined by recorded qualification results, not by the
> age of the public GitHub repository.

The two facts coexist: the engine and packaged release have passed the stated
technical gates, while the public production record and surrounding ecosystem
are still young. Calling 1.2.25 stable describes the first fact; publishing
this qualification boundary makes the second explicit.
