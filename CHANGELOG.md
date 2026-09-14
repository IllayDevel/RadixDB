# Changelog

[Русский](CHANGELOG.ru.md)

This file records user-visible source/API changes. Wire and storage
compatibility are governed by their dedicated version gates.

## Unreleased

## 1.2.19 - 2026-09-14

### Application interfaces

- Added the domain-neutral `radixdb-app-sdk` for trusted Rust services, with
  bounded request/session identity, deadlines, cancellation, result limits,
  stable outcome classification and generated schema-bound contracts.
- Added the explicit database `DESCRIBE` privilege for schema discovery.
- Preserved table-returning procedure cardinality in generated contracts and
  added deterministic table, procedure and application-event fingerprints.
- Expanded ORM contract coverage for binary predicates, NULL/BETWEEN/IN,
  subqueries, CASE, tuple expressions, ordering and parameter numbering.

### SQL types and constraints

- Added bounded `TEXT(n)` with `VARCHAR(n)` and `CHAR(n)` spellings.
- Added a distinct `DOUBLE PRECISION` SQL and catalog identity backed by f64.
- Added `TIMESTAMP` for civil date-time values without an implicit time zone.
- Added `TIME` for time-of-day values.
- Added `TIMESTAMPTZ` for instants, including checked IANA/fixed-offset session
  zone conversion and rejection of ambiguous or nonexistent DST local times.
- Made derived `DECIMAL` addition, subtraction and multiplication exact across
  different scales, with precision and overflow checks.
- Added scalar and ordered composite primary keys, including text components;
  every primary-key component is non-null and full-key uniqueness is enforced.
- Accepted qualified relation names in post-load constraints, batched cold
  uniqueness probes at bulk insert/commit, and split variable row groups by
  byte budget before publication.

### Server, programming and access control

- Added catalog-principal password authentication, database `CONNECT` checks,
  direct TLS, certificate reload and `radixdb-password`. Passwordless `root`
  remains available only as a plaintext loopback recovery path when no root
  verifier is configured.
- Added principal/role lifecycle operations, grant options, delegated role
  grantors, independent schema `USAGE`/`CREATE`, and trigger privilege checks.
- Added routine, trigger and job lifecycle commands, typed RadixDB PL contexts,
  checked dynamic identifiers and static `OLD`/`NEW` trigger records.
- Added the durable stock-server Job scheduler with leases, no-overlap runs,
  bounded retry/backoff, misfire coalescing, history and clean shutdown.

### Trusted native extensions

- Added stable C ABI 1.0, a safe Rust authoring SDK and deterministic package
  tooling for bounded external scalar types, scalar/batch functions, binary
  operators, B-tree/hash/bitmap operator classes and planner support.
- Added exact checksummed startup allowlists, transactional catalog 6.2
  bindings, extension identity in protocol values, and native package support
  for x86-64 and AArch64 GNU/Linux with ELF architecture admission.
- Added `radixdb-spatial` as a proving extension for geometry values,
  predicates, Morton-key B-tree access and residual recheck.
- Extension packages remain trusted in-process code. Network installation, hot
  reload, version ranges, `ALTER EXTENSION UPDATE`, aggregate/window/table
  function descriptors and extension-aware bundled backup/export are absent.

### Reliability and compatibility

- Added checksum-bound physical backup identity, deterministic logical export
  verification and bounded artifact runtime status.
- Preserved transaction isolation/savepoints in CLI batches and reject retired
  cache/compression settings instead of ignoring them.
- Hardened CompactArc final-owner synchronization under ThreadSanitizer and
  native extension ABI checks under AArch64/QEMU.
- Wire protocol 18 is not compatible with protocol 14 from RadixDB 1.1.0;
  upgrade server and clients together and use logical export/import across an
  unsupported physical-format boundary.

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
