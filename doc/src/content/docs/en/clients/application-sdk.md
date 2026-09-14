---
title: Application SDK
description: Build a trusted Rust application service with generated, schema-bound RadixDB contracts.
---

`radixdb-app-sdk` is the domain-neutral boundary for trusted Rust application
services. It combines the asynchronous RadixDB client, ORM documents and a
generated database contract without exposing engine internals or a generic
database endpoint to browsers.

```text
browser or API consumer
        -> application service and product authorization
        -> generated schema-bound Rust contract
        -> radixdb-app-sdk
        -> radixdb-client / radixdb-orm
        -> RadixDB server
```

The generic crate contains no application tables, procedures, routes, roles or
screens. A generated application crate is specific to one logical database
schema. The application service and its HTTP, authentication and user-interface
contracts remain product-specific.

## Dependencies

For RadixDB 1.2.21, add the SDK, asynchronous client and ORM to the
trusted service:

```toml
[dependencies]
radixdb-app-sdk = "1.2.21"
radixdb-client = { version = "1.2.21", features = ["tokio"] }
radixdb-orm = "1.2.21"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

During development from a source checkout, use `path` dependencies that all
point to the same RadixDB revision. Do not mix generated source from one
descriptor version with runtime crates from another release.

## Connect to the server

The application service authenticates with a database principal, selects the
database, and wraps `AsyncConnection` in `AsyncConnectionTransport`:

```rust
use std::{sync::Arc, time::{Duration, Instant}};

use radixdb_app_sdk::{
    ApplicationClient, ApplicationSession, AsyncConnectionTransport,
    RequestContext, RequestId,
};
use radixdb_client::{AsyncConnection, AsyncTimeouts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = AsyncConnection::connect(
        "127.0.0.1:15432",
        AsyncTimeouts::default(),
    ).await?;
    connection
        .authenticate_database("inventory", "inventory_service", "secret")
        .await?;
    connection.select_database("inventory").await?;

    let transport = AsyncConnectionTransport::new(connection);
    let mut client = ApplicationClient::new(transport);

    let session = Arc::new(ApplicationSession::new(
        "user-42",
        "session-7",
        3,
        ["inventory.read".to_owned(), "inventory.write".to_owned()],
    )?);
    let context = RequestContext::new(RequestId::new("request-1001")?, session)
        .with_deadline(Instant::now() + Duration::from_secs(2));

    let descriptor = client.describe_database(&context, 8 * 1024 * 1024).await?;
    std::fs::create_dir_all("schema")?;
    std::fs::write("schema/database.json", descriptor.to_json()?)?;
    Ok(())
}
```

`authenticate_database` establishes the RadixDB principal and its database ACL.
`ApplicationSession` carries the already authenticated product user. These are
separate authorities: constructing an application permission does not grant a
database privilege, and database authentication does not prove a browser user's
identity.

Create `ApplicationSession` only from trusted authentication state. Never copy
subject IDs, authorization revisions or permission sets directly from a request
body. Use a fresh `RequestId` for each request and propagate cancellation from
the application server into `CancellationSignal`.

## Schema discovery and generation

The database principal needs the explicit `DESCRIBE` privilege to retrieve a
database descriptor. It does not need table-read permission merely to describe
the schema:

```sql
GRANT DESCRIBE ON DATABASE inventory TO inventory_service;
```

Generate the application source from the descriptor envelope:

```console
cargo run --package radixdb-app-sdk --bin radixdb-app-codegen -- \
  schema/database.json src/generated.rs
```

Generation validates every table, procedure and database fingerprint before it
writes source. The portable fingerprint excludes physical catalog IDs,
generations, timestamps and procedure revision counters. Two independently
migrated databases therefore produce the same generated contract when their
logical schemas are identical.

The generated module contains:

- table records, typed columns, keys and navigation descriptors;
- typed procedure argument and result structures;
- parameter encoders and result decoders;
- `DATABASE_SCHEMA_FINGERPRINT` for startup compatibility checks;
- per-procedure fingerprints.

Keep both the descriptor and generated source in the application repository.
Regenerate in CI and reject an unexpected diff. At startup, obtain the live
descriptor with `describe_database`, calculate
`application_descriptor_fingerprints`, and compare its `schema` value with
`generated::DATABASE_SCHEMA_FINGERPRINT` before accepting traffic.

## Call a generated procedure

Suppose the database contains `inventory.reserve_stock(product_id UUID,
quantity INTEGER)`. The generated module exposes an
`InventoryReserveStockCall` with the corresponding Rust fields:

```rust
use std::time::{Duration, Instant};

use radixdb_app_sdk::{IdempotencyKey, RequestContext, RequestId};
use crate::generated::InventoryReserveStockCall;

let context = RequestContext::new(RequestId::new("request-1002")?, session)
    .with_idempotency_key(IdempotencyKey::new("reserve-stock:order-91")?)
    .with_deadline(Instant::now() + Duration::from_secs(2));

let result = client.call(
    &context,
    InventoryReserveStockCall {
        product_id: "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e90".to_owned(),
        quantity: 4,
    },
).await?;
```

The procedure identifier and positional encoding come from generated code, not
from browser input. The decoder checks result kind, cardinality, column order,
data types and nullability. A mismatch is a contract error rather than a partial
dynamic result.

Use typed procedures for atomic commands and typed ORM queries for reads. Do
not publish `ApplicationClient`, ORM IR or generated procedure names as a
generic HTTP endpoint.

## Request context

`RequestContext` is immutable after construction and contains:

- a bounded, non-empty request ID;
- a trusted `ApplicationSession`;
- an optional idempotency key;
- an optional monotonic deadline;
- a shared cooperative cancellation signal.

The SDK checks cancellation and deadline before transport execution. If either
fires after execution begins, the outcome is deliberately reported as unknown:
the database may have committed even though the service did not receive the
reply.

## Retry and outcome handling

Inspect both dimensions of `ApplicationError`:

```rust
use radixdb_app_sdk::{ApplicationError, OperationOutcome, RetryClass};

fn recovery_action(error: &ApplicationError) -> &'static str {
    match (error.retry_class(), error.outcome()) {
        (RetryClass::SafeAfterBackoff, OperationOutcome::RejectedBeforeCommit) => {
            "retry after bounded backoff"
        }
        (RetryClass::RequiresOutcomeResolution, OperationOutcome::Unknown) => {
            "reconcile by idempotency key before retrying"
        }
        _ => "return the classified error",
    }
}
```

Only explicit server backpressure is classified as safely retryable. Network
loss, protocol failure, timeout and cancellation after transport start require
outcome resolution. Check `client.is_reusable()` before returning its transport
to a pool; a poisoned or uncertain connection must be discarded.

An idempotency key is context metadata, not automatic deduplication. The called
procedure or application schema must persist and enforce the corresponding
deduplication contract.

## Bounded results

`TypedQuery::limits` and `TypedProcedure::limits` control both row count and
estimated encoded bytes. Defaults are 10,000 rows and 8 MiB. The hard ceilings
are 100,000 rows and 64 MiB. The transport closes an unfinished cursor before
returning `ResultLimitExceeded`.

Lower the limits for endpoints that need only one row or a small page. Do not
use the hard maximum as a substitute for application pagination.

## Events

`ApplicationEvent` declares only a serializable topic and version:

```rust
use radixdb_app_sdk::ApplicationEvent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct StockReserved {
    product_id: String,
    quantity: i64,
}

impl ApplicationEvent for StockReserved {
    const TOPIC: &'static str = "inventory.stock-reserved";
    const VERSION: u32 = 1;
}
```

The trait does not publish an event. Append the serialized event to the
transactional outbox in the same transaction as the business change, then let
the application service map committed outbox records to SSE, a queue or another
delivery system.

## Ownership boundary

`radixdb-app-sdk` owns transport-neutral application contracts. A generated SDK
owns one schema. The product repository owns authentication, authorization,
routes, scripts, UI and business rules. RadixDB does not depend on the product
and generated product code must not be copied into the engine repository.

See also [Rust TCP client](../rust-client/), [Rust ORM](../orm/),
[access control](../../administration/access-control/) and
[routines](../../programming/routines/).
