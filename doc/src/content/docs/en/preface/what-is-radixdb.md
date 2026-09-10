---
title: What Is RadixDB?
description: Purpose and operating modes of RadixDB.
---

RadixDB is an open-source SQL database written in Rust. It stores related data
in tables and provides queries and transactions for applications. The engine
can run inside a Rust application or in a separate process as a TCP server.

Its storage design combines mutable row versions with immutable compressed
column blocks. This allows transactional changes and column-oriented scans
within one database. Cache budgets control parts of memory use; they do not
require every database to fit into RAM.

## Operating Modes

An embedded application opens the database through the Rust API. The local
command-line client uses this mode and is the starting point of this tutorial.
A server owns the database in its process and accepts connections from clients.
The local CLI is not a TCP shell for the server.

The manual later introduces navigable foreign-key references, the client ORM,
procedural programming and trusted native extensions. These features serve
different purposes: query notation, application data access, server-side logic
and bounded domain-specific types, functions and index behavior.

Functions, procedures, triggers and extensions form a practical base feature
set rather than a promise of drop-in PostgreSQL compatibility. More specialized
database behavior can be composed from these mechanisms, although the syntax
or workflow may be less direct than in a larger system.

## Version and Origins

This manual documents the implemented RadixDB 1.2 boundary. Recorded benchmark
and recovery results apply to their tested commits and workloads, not
automatically to every installation or later version.

RadixDB is distributed under the Apache License, Version 2.0.

The official project website is [radixdb.org](https://radixdb.org). Development
is sponsored by [Light Soft](http://light-soft.info/), and user support is
available at [dev@radixdb.org](mailto:dev@radixdb.org).

Continue with [conventions](../conventions/) and the
[command-line tutorial](../../tutorial/getting-started/).
