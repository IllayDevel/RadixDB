---
title: Storage internals
description: MVCC hot state, V6 immutable artifacts, WAL, generation publication and recovery.
---

RadixDB 1.2 combines mutable MVCC state for current transactions with immutable
V6 artifacts for cold committed data. This chapter explains the lifecycle and
failure boundaries. Exact binary fields, checksums, limits and crash outcomes
are defined by the versioned codecs, format constants and executable recovery
tests in `radixdb-storage`.

## Hot and cold state

```text
transaction -> WAL + hot MVCC versions -> commit visibility
                         |
                         v
                   seal/checkpoint
                         |
          staged .data/.idx + manifests + catalog
                         |
                         v
                 committed CONTROL root
```

Recent inserts, updates and deletes live in version stores and are visible by
MVCC rules. The WAL makes committed changes recoverable before those hot rows
become cold. A checkpoint seals committed rows into immutable segments,
publishes a complete physical generation and advances the replay floor only
when all committed hot rows covered by that floor are durable in artifacts.

Compaction is independent maintenance after checkpoint durability. It may
merge sub-target segments, remove obsolete versions or split an oversized
segment, but it cannot silently become checkpoint or WAL-retention authority.

## One committed generation

The database root contains these owner classes:

| Class | Role | Authority |
| --- | --- | --- |
| `CONTROL.0`, `CONTROL.1` | Two fixed 4096-byte root slots | Select the newest completely reachable generation |
| `wal/` | Append-only active and retired generations | Replay DML and transactional catalog mutations after the selected floor |
| `catalog/` | Immutable typed catalog packs | Names, stable object IDs, dependencies and schema payloads |
| `manifests/` | Database and per-table immutable manifests | Bind catalog, WAL floor and exact segment membership |
| `artifacts/data/` | Immutable `.data` files | Authoritative row IDs, values, statistics and bloom filters |
| `artifacts/index/` | Immutable `.idx` files | Rebuildable exact, ordered and vector accelerators |
| `staging/` | Private complete publication sets | Candidate members before final placement |
| `snapshots/` | Independently retained roots | Physical backup membership |
| `quarantine/` | Rename-first garbage collection | Objects proven unreachable before deletion |

Durable membership comes only from a valid CONTROL graph or snapshot manifest,
never from directory enumeration. Paths are derived from checked identities;
the complete root must stay on one filesystem so final placement can use
same-filesystem atomic operations.

## Semi-columnar data artifacts

A table manifest names immutable row or tombstone segments. Each required
`.data` artifact stores row groups with independently addressable row-ID,
column and bloom blocks. Fixed-width values use plain blocks; TEXT, JSON and
BYTES may choose a smaller plain or dictionary representation. Stored blocks
may use raw LZ4 compression. Zone maps, distinct estimates, numeric summaries
and bloom filters remain bounded metadata inside `.data`.

This layout lets a scan decode selected columns and groups without expanding
every row into a permanent object. Metadata-only open reads directories and
statistics without reading value blocks. A point or range path can use an
`.idx` page and then fetch matching values from `.data`; a full scan can stay
column-oriented through eligible operators and the optional TCP column-batch
path.

`.data` is authoritative. Missing or corrupt required data invalidates that
candidate generation. `.idx` is an accelerator: an unavailable retained index
is reported and exact reads fall back to a semantically equivalent scan where
allowed. A newly created corrupt index is still a publication error; the writer
may not commit an artifact it already knows is invalid.

## Atomic publication

```text
prepare and validate successor graph
        -> write and fsync staged members
        -> move immutable members and fsync parent directories
        -> publish table manifests, catalog, WAL successor, database manifest
        -> replace and fsync the inactive CONTROL slot last
        -> swap the validated runtime generation
        -> retire old WAL and collect unreachable members later
```

One filesystem publisher is bound to the canonical root identity, database ID
and the retained OS writer lock. It rejects another, cloned, moved or replaced
root before mutation. A complete staging marker binds the exact candidate
member set; retry may reuse identical files already moved to final paths but
cannot infer membership from their presence.

The short publication fence rechecks the source CONTROL before any final side
effect. Concurrent publishers serialize; a prepared loser becomes stale and
does not overwrite the winner. CONTROL is the last durable commit point. A
crash before it leaves the older generation selected; a crash after it leaves
the complete successor selected.

## Open and recovery

Startup acquires the writer lock before recovery and validates both CONTROL
slots independently. Candidates are considered newest first, but only a fully
reachable and cross-checked catalog/manifest/data/WAL graph can win. An
incomplete newer graph does not hide an older complete generation. Equal
generation numbers with different identities are split-brain corruption and
fail closed.

For a candidate, recovery first validates the immutable graph and replays
committed catalog WAL entries after its floor. After selecting one candidate,
the same WAL owner restores DML/MVCC state once. An incomplete tail is ignored;
complete corruption or a resource-limit breach is an error. Recovery never
reconstructs CONTROL or manifests by scanning filenames.

Readers pin the selected immutable generation while a query uses it. GC can
retire a member only after it proves the object unreachable from every valid
CONTROL and snapshot root and no in-process lease remains. See
[Storage architecture](../../administration/storage/) for operator behavior and
[Backup and restore](../../administration/backup-restore/) for retained copies.
