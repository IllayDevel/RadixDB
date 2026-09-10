---
title: Release Notes
description: User-visible changes, identity and compatibility boundaries of RadixDB releases.
---

This page summarizes user-visible releases. Git tags and `CHANGELOG.md` are the
release authority; feature chapters describe the detailed contract.

## 1.2 - Development

The 1.2 manual follows the current 1.2 source line and wire protocol 17. Until
release metadata is finalized, binaries built from this line still report the
Cargo package version 1.1.0; use the full build identity to distinguish them
from the 1.1 release.

### Server security and access control

The stock server can authenticate durable database Principals, verifies
Argon2id password hashes and checks database `CONNECT` before admitting a
session. Direct TLS is available as an alternative to plaintext TCP, with CA
and hostname validation, strict private-key permissions and atomic certificate
reload for new connections. Passwordless `root` is limited to plaintext
loopback as a recovery path when no root verifier is configured. The new
`radixdb-password` utility generates a validated Argon2id PHC verifier; once it
is configured, the original password is mandatory for `root` on plaintext or
TLS endpoints and passwordless login is disabled everywhere.

Principal and Role objects now support enable, disable, rename and dependency-
aware drop. Object and column grants support `WITH GRANT OPTION` and
`REVOKE GRANT OPTION FOR`; delegated Role membership records its grantor, and
schema `USAGE` is independent from schema `CREATE`. Trigger execution checks
schema and function privileges when a trigger is attached and each time it
fires.

### Server programming

Functions, Procedures, Triggers and Jobs have explicit alter and drop
lifecycle. RadixDB PL exposes typed session, effective-principal, transaction,
request, statement-time and Job context values. `SQL_IDENTIFIER(TEXT)` provides
a checked identifier for dynamic SQL, while `:OLD.column` and `:NEW.column`
bind trigger records in static SQL.

The stock server now runs durable scheduled Jobs with leases, no-overlap
execution, retry with bounded exponential backoff, misfire coalescing, bounded
history and clean shutdown. Attempt failures use a stable diagnostic class so
applications do not need to parse error prose.

### Trusted native extensions

Version 1.2 adds a stable C ABI 1.0, a safe Rust authoring SDK and deterministic
package tooling for operator-trusted in-process extensions. A package admitted
at startup can expose bounded external scalar types, native scalar and batch
functions, binary operators, B-tree/hash/bitmap operator classes and bounded
planner support. SQL binds those exports transactionally to catalog 6.2, while
protocol 17 preserves external type identity and codec revision on the wire.

The host retains storage, WAL, MVCC, catalog mutation, index pages, ACL and
recovery ownership. Packages come only from exact absolute allowlist entries;
network install, hot reload, version ranges and `ALTER EXTENSION UPDATE` are not
available. The `radixdb-spatial` proving extension exercises fixed and variable
geometry types, native predicates, Morton-key B-tree indexing and residual
recheck through the public SDK.

The bundled CLI backup and logical-export workflows do not yet load a plugin
allowlist and are therefore not a supported recovery path for an
extension-bound database.

Start with [installing extensions](../../administration/extensions/), then use
the [developer guide](../../programming/native-extensions/) and
[`cargo radixdb-plugin`](../../reference/programs/cargo-radixdb-plugin/).

### Reliability and operations

External physical backups record checksum-covered build, format, database and
exact snapshot identity, and restore only that snapshot. Logical exports use a
normalized content checksum and can be reproduced, imported into a new root,
reopened and exported again. Runtime artifact status is bounded and reports
when its result was truncated.

The CLI preserves transaction isolation and savepoints in batches, rolls back
a failed batch, and displays the effective durability configuration. Retired
cache and compression settings are rejected instead of being silently ignored.
Nullable indexed window partitions retain the `NULL` group after cold storage
and reopen.

Protocol 17 is not wire-compatible with protocol 14. Upgrade server and client
as one tested set and use logical export/import when crossing an unsupported
physical-format boundary.

## 1.1.0 - 2026-09-08

RadixDB 1.1.0 is identified by annotated tag `v1.1.0` and uses wire protocol
14. It established the first released procedural and ACL foundation described
below. The limitations in this section apply to 1.1, not to the 1.2 development
line above.

### Procedural database foundation

Version 1.1 adds durable catalog objects for principals, roles, ACL entries,
functions, procedures, triggers and jobs. The bounded procedural runtime uses
the existing SQL parser, executor, transaction owner and MVCC/WAL path. It
includes typed calls, local variables, control flow, cursors, exception regions,
dynamic `EXECUTE`, transactional DML triggers and durable job-attempt records.

Invoker/definer contexts and role/object privileges are enforced in the
database execution path. Transactional application writes can publish audit
and outbox rows atomically, and bounded public ORM reads use the same authority.
See [PL/SQL](../../programming/pl-sql/), [routines](../../programming/routines/),
[triggers](../../programming/triggers/), [jobs](../../programming/jobs/) and
[access control](../../administration/access-control/).

### Storage and compatibility hardening

Normal rollback remains outside the synchronous durability deadline, while
rollback-marker failures are propagated. Publisher ownership is revoked on
close and same-process lock handoff is hardened.

A catalog 6.0 database opens without an automatic rewrite. The first
procedural or ACL DDL atomically promotes the catalog to exact minor 6.1. There
is no downgrade writer path, and a binary that knows only 6.0 rejects 6.1 fail
closed. Protocol 14 remains the wire contract for this release.

Review [upgrading](../../administration/upgrading/) before changing binaries,
and use the [SQL matrix](../compatibility/) rather than assuming PostgreSQL
syntax or wire compatibility.

### Accepted evidence and limits

The release gate preserved the 100,000,000-row checksum and access paths. The
largest listed query regression was 4.91%, below the frozen 1.20-times corridor.
The exact method and source attribution are in [benchmarks](../benchmarks/).

The stock 1.1 server remains loopback-only and does not provide principal login
or transport encryption. ACL is meaningful through trusted embedded or
authenticated gateway contexts, not as direct untrusted TCP authentication.
The stock server also does not run a background Job scheduler. These are
explicit product boundaries, described in [authentication](../../administration/authentication/)
and [jobs](../../programming/jobs/), not features implied by the catalog model.

## 1.0.0 - 2026-09-07

Version 1.0.0 established the catalog-artifact V6 production architecture and
accepted the 100M NVMe, 20k memory and six-hour HDD evidence. It also introduced
the transport-independent ORM v1 with versioned IR, canonical JSON, stable
schema descriptors, typed parameters and embedded/TCP execution extensions.

Existing data from older unnamed-catalog layouts is not rewritten in place.
Migration uses explicit logical export/import into a separately verified
destination. Consult [upgrading](../../administration/upgrading/) and
[backup and restore](../../administration/backup-restore/) before migration.

## Reading release notes

Release membership does not turn a parser-recognized form into a supported SQL
contract. Check the target release, compatibility matrix and documented
limitations independently.
