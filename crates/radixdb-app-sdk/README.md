# radixdb-app-sdk

`radixdb-app-sdk` is the domain-neutral application boundary above
`radixdb-client` and `radixdb-orm`. It is intended for trusted Rust services
that expose a product-specific API while RadixDB runs as a separate server.

The crate provides:

- immutable request and application-session context;
- cooperative cancellation and deadlines;
- idempotency keys and conservative retry/outcome classification;
- typed query, procedure and committed-event contracts;
- bounded row and byte limits;
- an async client facade and an adapter for `AsyncConnection`;
- deterministic Rust generation from a fingerprinted database descriptor.

Schema-specific generated crates implement the typed contracts. This crate
contains no application table, procedure, route, role or screen identifiers
and has no dependency on the RadixDB executor, catalog, storage, server or web
pages.

## Architecture

```text
browser or API consumer
        -> application service and product authorization
        -> generated schema-bound Rust contract
        -> radixdb-app-sdk
        -> radixdb-client / radixdb-orm
        -> RadixDB server
```

The SDK is not a public web endpoint. Do not accept SQL, ORM IR, procedure
names, application identities or permission sets supplied by a browser. The
application service authenticates its caller, constructs a trusted
`ApplicationSession`, and exposes only product-specific operations.

## Connect and create a request context

```rust
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use radixdb_app_sdk::{
    ApplicationClient, ApplicationSession, AsyncConnectionTransport, RequestContext, RequestId,
};
use radixdb_client::{AsyncConnection, AsyncTimeouts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connection =
        AsyncConnection::connect("127.0.0.1:15432", AsyncTimeouts::default()).await?;
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

    let descriptor = client
        .describe_database(&context, 8 * 1024 * 1024)
        .await?;
    println!("catalog fingerprint: {}", descriptor.payload.fingerprint);
    Ok(())
}
```

The database principal used by `authenticate_database` and the application
identity in `ApplicationSession` are separate authorities. RadixDB ACL protects
the service connection. The service must enforce its own user and permission
model before invoking a generated operation.

## Generate a schema-bound contract

Grant the service account `DESCRIBE` on the database, fetch the canonical
database descriptor with `ApplicationClient::describe_database`, serialize it
with `DescriptorEnvelope::to_json`, and generate Rust source:

```console
cargo run --package radixdb-app-sdk --bin radixdb-app-codegen -- \
  schema/database.json src/generated.rs
```

Generation validates table, procedure and database fingerprints before writing
source. Physical catalog IDs, generations and timestamps are removed from the
portable fingerprint, so two independently migrated databases with the same
logical schema produce the same application contract. Structural changes do
change the fingerprint and should fail application startup until bindings are
regenerated and reviewed.

The generated module contains table records, typed columns and keys, the
`DATABASE_SCHEMA_FINGERPRINT` constant, and typed procedure calls. Keep the
descriptor and generated source under version control and regenerate them in
CI to detect drift.

## Invoke generated procedures

```rust
use std::time::{Duration, Instant};

use radixdb_app_sdk::{IdempotencyKey, RequestContext, RequestId};
use crate::generated::InventoryReserveStockCall;

let context = RequestContext::new(RequestId::new("request-1002")?, session)
    .with_idempotency_key(IdempotencyKey::new("reserve-stock:order-91")?)
    .with_deadline(Instant::now() + Duration::from_secs(2));

let result = client
    .call(
        &context,
        InventoryReserveStockCall {
            product_id: "product-7".to_owned(),
            quantity: 4,
        },
    )
    .await?;
```

Procedure names and parameter encoders come from the generated contract, not
from request data. Generated decoders validate result cardinality, column
order, types and nullability.

## Retry and cancellation rules

`ApplicationError` exposes both `RetryClass` and `OperationOutcome`:

- `Never` means the same request must not be retried automatically;
- `SafeAfterBackoff` is returned only when the server says the operation was
  rejected before commit;
- `RequiresOutcomeResolution` means a timeout, cancellation or transport loss
  may have happened after execution started.

For an unknown outcome, reconcile using the operation's durable idempotency key
or business identifier before deciding whether to submit another command. Also
check `ApplicationClient::is_reusable()` before returning a connection to a
pool.

Cancellation is cooperative. A context cancelled before execution never
reaches the transport. Cancellation or deadline expiry after execution begins
is conservatively classified as an unknown outcome.

## Result limits

Every typed query and row-returning procedure has row and encoded-byte limits.
Defaults are 10,000 rows and 8 MiB. `ResultLimits::new` can lower them or raise
them up to the hard bounds of 100,000 rows and 64 MiB. Exceeding either limit
closes the cursor and returns `ResultLimitExceeded`.

## Events

`ApplicationEvent` defines a serializable topic/version contract. It does not
create a message broker or publish on its own. Applications normally append a
committed event to the RadixDB transactional outbox and let their service map
that event to SSE, a queue or another product-specific delivery mechanism.

The complete guide is available in the RadixDB manual under **Client
interfaces -> Application SDK**.
