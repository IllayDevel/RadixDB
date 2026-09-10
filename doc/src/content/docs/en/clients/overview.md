---
title: Client Interfaces
description: Choose where the engine runs and how an application accesses data.
---

Choose an interface first by where the database engine will run. An ORM is
an application abstraction, not a third deployment mode.

| Interface | Engine location | Application responsibility |
| --- | --- | --- |
| Embedded Rust API | Inside the application process | Open the database, execute operations and close it |
| Rust TCP client | In a separate RadixDB server process | Connect, authenticate, select a database and consume results |
| ORM | Uses a database execution interface | Describe models or records and choose transaction boundaries |

## Embedded Access

The local CLI used at the start of the tutorial demonstrates embedded access:
it opens a memory or file database without a network service. Rust applications
use the public database API provided by `radixdb-api` (also exposed through the
top-level `radixdb` crate). The application owns the engine lifecycle and needs
access to the database files.

## TCP Access

`radixdb-client` speaks the RadixDB binary protocol. It does not embed the
storage engine. Use a client compatible with the server protocol; do not assume
PostgreSQL clients or a PostgreSQL connection URL can connect to RadixDB.

The [server exercise](../../tutorial/server-connection/) runs a supplied client.
Its source is `examples/public/rust-client/basic.rs`: connection, authentication,
database selection, SQL execution, cursor fetching and connection shutdown.
Fetch until EOF or close an unfinished cursor before sending an unrelated command.
The local CLI's database path and the server's database name are not interchangeable.

## ORM

`radixdb-orm` provides model and record abstractions for application code.
It does not remove the need to understand SQL constraints, transaction boundaries
or errors. Choose embedded versus server deployment separately from whether
the application uses ORM operations. See [Embedded Rust](../embedded-rust/),
the [Rust TCP client](../rust-client/) and the [Rust ORM](../orm/) for the
detailed contracts and executable examples.

## Scope of This Overview

This overview is not the complete API contract. The detailed chapters define
parameter types, prepared statements, transaction errors and uncertain outcomes
after a lost connection. In particular, do not automatically retry a write
merely because its reply was not received.
