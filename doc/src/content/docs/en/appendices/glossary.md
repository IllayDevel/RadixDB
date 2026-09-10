---
title: Glossary
description: Terms used in the RadixDB manual.
---

This glossary establishes the terminology used throughout the manual.
It does not define the complete storage or transaction contract.

| Term | Meaning |
| --- | --- |
| Database root | Directory containing the persistent state of a database. |
| Catalog | Database metadata describing objects such as tables and indexes. |
| Transaction | A unit of work with a defined commit or rollback boundary. |
| MVCC | Multiversion concurrency control; readers observe versions according to visibility rules. |
| WAL | Write-ahead log used by the engine's durability and recovery procedures. |
| Checkpoint | A recovery boundary whose exact meaning is defined by the storage contract. |
| Cache budget | A configured bound for a cache, not a bound on total process memory. |
| RSS | Resident set size: the process memory currently resident in physical memory. |
| Foreign key (FK) | A relationship constrained by a reference to a key in another or the same table. |
| Navigable reference | RadixDB query notation for following declared relationships. |
| ORM | Object-relational mapping: an application interface relating records and database operations. |
| ACL | Access control list; the object privilege model described by the security chapters. |
| Invoker / definer | The caller or owner context used to evaluate routine permissions. |
| Accepted | Behavior verified by the implementation's acceptance checks, not merely described in a plan. |

SQL keywords, configuration keys and API identifiers are not translated.
Specific limits, isolation rules and security guarantees belong to their
respective chapters rather than these short definitions.
