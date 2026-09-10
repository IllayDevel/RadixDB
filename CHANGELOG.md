# Changelog

[Русский](CHANGELOG.ru.md)

This file records user-visible source/API changes. Wire and storage
compatibility are governed by their dedicated version gates.

## Unreleased

## 1.1.0 - 2026-09-08

### Procedural database foundation

- Added durable catalog 6.1 objects for principals, roles, ACL entries,
  functions, procedures, triggers and jobs, with atomic `6.0 -> 6.1`
  promotion and fail-closed old-binary handling.
- Added the verified, bounded RadixDB procedural runtime over the existing SQL
  parser, executor and MVCC owner, including typed calls, cursors, exception
  regions, dynamic `EXECUTE`, transactional DML triggers and durable jobs.
- Added invoker/definer authorization, role and object privileges, atomic
  business/audit/outbox operations and bounded public ORM reads.
- Added crash/reopen, publication failpoint, resource exhaustion, cancellation,
  concurrent DDL/DML/CALL/REVOKE and catalog format gates.

### Storage and compatibility hardening

- Kept normal rollback outside the synchronous durability deadline while
  preserving explicit rollback-marker failure propagation.
- Hardened publisher-lock revocation on close and same-process lock handoff.
- Preserved the accepted 100-million-row cardinality, checksum and access paths;
  all measured release cases stayed inside the frozen `1.20x` corridor.

## 1.0.0 - 2026-09-07

### Release baseline

- Accepted the catalog-artifact V6 production architecture and its canonical
  completion gates.
- Accepted the 100M NVMe correctness/performance evidence and the 20k
  micro-device memory profile.
- Accepted CA-90.3 after a complete 100M/6h Atom/HDD run: 2,351,035
  operations, 2,100 successful invariant checks, final snapshot/restore/digest
  verification, and a clean process exit.
- Preserved the live recovery evidence from deliberate I/O starvation and an
  unplanned ATA `FLUSH CACHE EXT` bus reset on the degraded HDD stand.
- Deferred 24h/72h endurance runs to a separate healthy-hardware program; they
  are not blockers for this release baseline.

### Client ORM v1

- Added the transport-independent `radixdb-orm` crate with versioned IR,
  canonical JSON, JSON Schemas, deterministic SQL rendering, typed parameters,
  dynamic records, GUI descriptors, and offline Rust code generation.
- Added ORM execution extensions to the existing embedded database and Rust TCP
  clients. Raw SQL and ORM share one connection and transaction owner.
- Added deterministic automatic constraint names, stable constraint identities,
  transactional `ALTER TABLE ... DROP CONSTRAINT`, and full JSON
  `DESCRIBE TABLE/DATABASE` descriptors.
- Added owner-typed generated columns and one-column PRIMARY/UNIQUE NOT NULL key
  descriptors. `Reference<T>` remains key-only; cascade-save and reverse
  collections are deliberately absent.
- Published language-neutral conformance fixtures for every accepted ORM
  contract example and a standalone Rust quickstart package.
- Existing unnamed-catalog data is not rewritten in place. Migration requires
  an explicit logical export/import into a separately verified destination.
