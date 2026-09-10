---
title: Storage Architecture
description: Hot MVCC rows, immutable V6 data artifacts, row groups, compression, checkpoints and compaction in RadixDB 1.2.
---

RadixDB 1.2 uses a hybrid storage model. Recent changes live in mutable MVCC
state; checkpoints seal committed rows into immutable, checksummed data and
index artifacts. Queries combine both parts under one transaction snapshot.
The split is an implementation detail: applications still read and modify SQL
tables rather than choosing a row or column store.

This chapter describes the V6 implementation used by the RadixDB 1.2 source
baseline `23bf35df011aae6816d77578be96074b02bc363c`. It does not describe the
retired 0.5.x storage format.

## Hot and cold state

The two storage parts have different jobs:

| Part | Representation | Purpose |
| --- | --- | --- |
| Hot | Mutable MVCC rows and transaction versions | Low-latency inserts, updates, deletes and snapshot visibility |
| Cold | Immutable V6 DATA artifacts plus INDEX artifacts | Compact durable storage, bounded reads and column-oriented scans |

A committed hot row remains visible while sealing is in progress. Publication
switches the durable physical generation atomically; readers pin a generation
instead of observing a partly replaced artifact set. Tombstones and hot
versions are merged with cold rows so a scan does not return superseded data.

Do not open one file database simultaneously through the server and an embedded
process. There is one storage owner for its WAL, manifests and generations.

## Seal and checkpoint

The server release starts a first seal near `67108864` estimated hot bytes per
table and later incremental seals near `16777216` bytes for a table that already
has cold segments. These thresholds initiate maintenance; they are neither row
limits nor hard process-memory ceilings.

`target_volume_rows = 1048576` shapes newly sealed and compacted outputs. The
writer aligns output to 65,536-row groups, so the release target is normally
about 16 complete groups. A final group may be shorter. Changing the target
affects new outputs, not artifacts already published.

A checkpoint coordinates hot-row sealing, catalog state, manifests and WAL
progress. Request one through SQL when an operational procedure requires a
known durable generation:

```sql
PRAGMA CHECKPOINT;
PRAGMA VOLUME_STATS;
```

Normal shutdown also performs a final checkpoint in the default persistence
configuration. A successful checkpoint is not a backup by itself; the backup
chapter defines how to copy a self-consistent generation.

## Immutable V6 artifacts

Each cold table segment is represented by immutable DATA and, where applicable,
INDEX artifacts. Catalog and database/table manifests identify the committed
generation and its members. Checksums protect headers, directories and stored
payloads; readers reject malformed bounds, unsupported tags, checksum failures
and inconsistent cross-references.

Artifact filenames and directories are owned by the engine. Do not rename,
replace, copy or delete individual files while the database is open. A directory
listing is not a consistency protocol because staged, superseded and snapshot
files can coexist with the selected generation.

## Row groups and columns

A DATA artifact divides rows into groups of at most 65,536. Each group contains
a row-ID block, one block for every stored column and optional Bloom-filter
blocks. Directories describe columns, groups, blocks and statistics before a
reader allocates or decodes payload memory.

Fixed-width values use canonical little-endian representations. Variable-width
`TEXT`, `JSON` and `BYTES` blocks can use plain or dictionary layout; the writer
chooses the smaller stored candidate for each block. Validity bitmaps represent
NULL independently of value bytes.

Per-group statistics and Bloom data can reject irrelevant groups. A projected
scan can decode only selected columns. These properties explain why the cold
path is column-oriented, while hot transactional state remains row-oriented.
They do not imply spatial indexes or every PostgreSQL access method.

## Compression

The default persistence profile enables raw-block LZ4 for cold DATA artifacts.
The format also permits uncompressed blocks. Decompression validates the
declared logical length and refuses invalid ratios or bounds before allocating
the result; the checksum covers stored bytes.

The 1.2 server TOML does not expose a compression switch. Embedded file DSNs do
accept `volume_compression`. The former `compression_threshold` placeholder was
removed because seal and compaction never implemented its advertised behavior;
supplying it now fails closed as an unknown option. Compression therefore has
one honest contract: enabled or disabled for newly written cold volumes.

Compression ratio depends on types, cardinality and value distribution. Size a
deployment from a representative load, checkpoint and reopen rather than from
a universal ratio.

## Compaction

Seals create immutable level-zero segments. Background compaction merges
eligible segments, applies tombstones and publishes replacement artifacts. One
job owns one table; the release permits one job at a time. Input/output byte
budgets, disk reserve and level-zero backpressure protect publication, although
only a subset is exposed by the current server TOML.

Larger `target_volume_rows` can reduce per-volume metadata and improve
compression, but may increase rewrite work. Smaller outputs reduce each rewrite
at the cost of more artifacts and metadata. Keep the release value until a
representative ingest, scan and compaction cycle has been measured on the target
filesystem.

## Inspecting storage

`PRAGMA VOLUME_STATS` returns one row per cold segment, including tier, row
count, resident memory components, idle cycles and tombstones. `PRAGMA
RUNTIME_STATS` returns one JSON value with hot/cold totals and maintenance
state. These are live observations and may change immediately after the query.

```sql
PRAGMA VOLUME_STATS;
PRAGMA RUNTIME_STATS;
```

The server protocol's `database_status(name)` reports whether filesystem
inventory was complete and counts WAL, artifact, snapshot, checkpoint and
manifest files. It does not report their byte size. For host capacity planning,
measure the complete database root only after a checkpoint, and leave separate
headroom for WAL, snapshots, staging, compaction output and the filesystem.

## Measured footprint

A recorded 100-million-row comparison at revision `b648b2d3` measured one
RadixDB database at `1964220021` logical bytes and the corresponding PostgreSQL
18.3 database at `16126596799` bytes. The run used a fixed relational fixture on
Ryzen 9 7950X, NVMe and Btrfs with page-cache warmup disabled. That is an
approximately 8.21-fold difference for that dataset, not a general compression
guarantee and not a measurement of `23bf35df`.

The [memory chapter](../memory/) separates disk footprint, process RSS and the
operating-system file cache. [Server configuration](../configuration/) defines
the knobs currently accepted by `server.toml`. Do not improvise a backup from
individual artifacts while the dedicated backup and restore chapter is pending.
