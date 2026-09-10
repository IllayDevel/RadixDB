---
title: RadixDB Manual
description: User manual for RadixDB 1.2.
---

This development manual targets **RadixDB 1.2**. The application version, Git
revision and working-tree state used for each build are shown in the page
footer. Known limitations are stated where they affect a command, interface or
procedure.

## Contents

The reading sequence begins with a local database in the command-line client,
continues with SQL and administration, and then introduces application interfaces
and server programming. The reference provides detailed syntax and parameters.

The manual follows this sequence:

1. [Introduction](./preface/what-is-radixdb/): purpose, terminology and conventions.
2. [Tutorial](./tutorial/getting-started/): first database, queries, relationships and transactions.
3. [SQL language](./sql/syntax/): types, expressions, queries, indexes and navigable references.
4. [Administration](./administration/installation/): installation, configuration, storage and recovery.
5. [Client interfaces](./clients/overview/): embedded Rust, TCP client and ORM.
6. [Server programming](./programming/pl-sql/): procedural SQL, routines, triggers, jobs and trusted native extensions.
7. [Reference](./reference/sql/): SQL commands, programs and configuration parameters.
8. [Internals](./internals/overview/): query execution, storage and protocol.

## Appendices

- [Compatibility matrix](./appendices/compatibility/)
- [Limits](./appendices/limits/)
- [Benchmarks](./appendices/benchmarks/)
- [Release notes](./appendices/release-notes/)
- [Glossary](./appendices/glossary/)

## Version and Verification

The documentation target is 1.2 development. The application version, Git
revision and working-tree state are displayed separately so that a documentation
build does not silently claim a released engine. Procedural SQL, ACL and native
extension chapters describe the current 1.2 boundary, including explicitly
stated restrictions. Native packages execute in-process and require operator
trust; begin with [extension operation](./administration/extensions/).

## Project

The official project website is [radixdb.org](https://radixdb.org). For support,
write to [dev@radixdb.org](mailto:dev@radixdb.org). Development of RadixDB is
sponsored by [Light Soft](http://light-soft.info/).
