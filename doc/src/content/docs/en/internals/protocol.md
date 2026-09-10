---
title: Wire protocol
description: Protocol 17 framing, session states, cursors, external values and compatibility.
---

RadixDB 1.2 server and TCP clients share one binary contract owned by the
private `radixdb-protocol` crate. This is RadixDB protocol 17, not the
PostgreSQL wire protocol. Applications use `radixdb-client`; direct codec use
is an internal integration boundary.

## Frame and negotiation

```text
4-byte unsigned big-endian payload length
        + one bincode-standard ClientMessage or ServerMessage payload
```

A zero length is invalid. Before allocating, each peer rejects a payload above
the negotiated frame limit. Client and server start with a 64 MiB protocol cap;
the handshake selects the minimum of the client request, server setting and
that cap. Decoding has a separate 256 MiB allocation ceiling and must consume
the complete payload. The server also applies its configurable process-wide
in-flight frame-byte budget.

Protocol enums use bincode's standard configuration and are not
self-describing. The first client message must therefore name exactly protocol
17. A different version is rejected rather than decoded as a nearby layout.

## Session state machine

```text
TCP connected
  -> Handshake / HandshakeAccepted
  -> Authenticate or AuthenticatePrincipal / AuthenticationAccepted
  -> Ready
       -> SelectDatabase when using bootstrap authentication
       -> Execute or Prepare + ExecutePrepared
       -> Fetch or FetchColumnBatch until EOF
       -> transaction and savepoint commands
       -> status, cancellation and close commands
```

Handshake negotiates the frame limit and capabilities. Authentication must
precede all ready-state commands. `AuthenticatePrincipal` authenticates against
one database and selects it atomically; that session cannot switch to another
database. Bootstrap `Authenticate` requires a later `SelectDatabase`. Repeating
handshake or authentication after the ready transition is a protocol violation.

A session keeps at most one active cursor. The caller must fetch it to EOF,
close it or cancel it before executing another statement, changing database or
starting conflicting state. Prepared statement IDs and cursor IDs belong to
their connection and are not portable between connections.

## Requests and responses

| Area | Client messages | Principal server outcomes |
| --- | --- | --- |
| Setup | `Handshake`, `Authenticate`, `AuthenticatePrincipal`, `SelectDatabase` | Accepted state or typed failure |
| SQL | `Execute`, `Prepare`, `ExecutePrepared`, `ClosePrepared` | `CommandComplete`, `CursorOpened`, prepared lifecycle |
| Results | `Fetch`, `FetchColumnBatch`, `CloseCursor`, `Cancel` | `RowBatch`, `ColumnBatch`, terminal close/failure |
| Transactions | Begin, commit, rollback and three savepoint operations | Explicit success or `TransactionFailed { active }` |
| Control | `CancelExecution`, `ServerStatus`, `CloseDatabase` | Cancellation result, bounded status or close result |

Each execution has a non-zero request ID. Out-of-band cancellation is sent on
another authenticated connection and addresses an active request by that ID.
The response says whether the request was found; cancellation remains
cooperative and does not itself prove whether a concurrently finishing command
published.

## Rows and column batches

Every cursor starts with ordered column metadata. The baseline result path is
row-major and carries the complete scalar wire domain: NULL, signed and
unsigned integers, floating point, DECIMAL, UTF-8 text, bytes, DATE, DATETIME,
nanosecond TIMESTAMP, JSON, packed `f32` VECTOR and UUID.

`ColumnBatchV1` is an optional negotiated capability for eligible
artifact-backed scans. It carries typed integer, float, boolean, timestamp,
dictionary-text, byte and JSON columns without allocating a row object per
cell. Filtering, MVCC overlays, schema mapping, ordering or unsupported value
types force a normal `RowBatch`; that fallback preserves query semantics.

`BuildIdentityV1` adds the semantic version, Git revision, protocol version,
profile and target to `ServerStatus`. Status collection is bounded by server
limits and may mark artifact counts incomplete rather than claiming a full
scan.

## External values

`ExternalValueV1` admits values created by catalog-bound native extensions. A
row value carries `type_object_id`, nonzero `codec_version` and canonical
`payload`. An external column batch carries the same type identity plus packed
data, offsets and NULL markers. Column metadata repeats the identity so the
client validates every value before exposing it.

The type object ID is derived from package UUID and stable local ID. It is not
the SQL type name and does not change when the schema object is renamed. The
payload limit is 16 MiB and can be lower in the type descriptor. Protocol 17
never falls back from an external value to `BYTES`; a client without the
capability receives `UnsupportedType`.

The low-level Rust client re-exports `WireValue::External`. A high-level ORM
does not guess how to decode extension bytes: applications need a plugin-aware
adapter whose identity and codec revision agree with `DESCRIBE DATABASE`.

## Failure and retry boundary

Protocol failures have stable classes such as authentication, authorization,
SQL, cursor, transaction state, commands-out-of-sync and server error. Only an
explicit `CompactionBackpressure` response currently proves the logical action
did not publish and is marked retryable. A read/write timeout, EOF or cancelled
client future leaves the outcome unknown; the client poisons the connection so
it cannot return to a pool.

`TransactionFailed` includes whether the transaction remains active. The
client must follow that field instead of guessing whether rollback is still
possible. Result batches are shape- and type-checked against the metadata from
`CursorOpened` before becoming caller-visible.

## Security and compatibility

Protocol authentication accepts database Principals and checks `CONNECT`
before a session becomes ready. The stock server can expose either plaintext
TCP or direct TLS. TLS protects the complete protocol from the first byte and
validates the issuing CA and server name; there is no STARTTLS negotiation or
plaintext downgrade. Passwordless `root` is restricted to plaintext loopback
as a recovery path when no server-side root verifier is configured. A configured
verifier makes the password mandatory for `Authenticate` on every transport and
bind address. See [Authentication](../../administration/authentication/) for
deployment guidance.

Client and server should be built from the same accepted revision. A protocol
version match is necessary but does not promise independent package lifecycle
or forward compatibility for unnegotiated enum variants. During upgrade, verify
the server build identity and run the client connection probe described in
[Rust TCP client](../../clients/rust-client/).
