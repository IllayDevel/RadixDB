---
title: Native Extensions
description: Build typed Rust extensions with the safe SDK, generated ABI adapters and reproducible packages.
---

The RadixDB 1.2 extension SDK lets an author write ordinary typed Rust while
procedural macros generate the C-compatible ABI boundary. Application source
does not need raw pointers, numeric ABI tags, descriptor arrays or
`extern "C"` functions.

Native extensions are appropriate for bounded domain types, computational
functions, operators and indexable predicates that need native performance.
They are trusted process code, not a mechanism for running untrusted modules.

## Project skeleton

A plugin is a standalone Cargo project with one `cdylib`, a lockfile and one
normal dependency: the public `radixdb-plugin` SDK from the matching RadixDB
source line. The current source distribution uses a path dependency; replace
the path only with an SDK artifact supplied for the same plugin ABI.

```toml
[package]
name = "radixdb-pair-plugin"
version = "1.0.0"
edition = "2021"
rust-version = "1.97"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
radixdb-plugin = { path = "../../../crates/radixdb-plugin" }

[workspace]

[profile.release]
panic = "unwind"
```

Direct normal or build dependencies on `radixdb-plugin-abi`, the macro crate,
the engine, catalog, executor or storage crates are rejected by the official
tool. Test-only development dependencies may provide a separate integration
harness, but they are not linked into the production `cdylib`.

Use this initial layout:

```text
radixdb-pair-plugin/
  Cargo.toml
  Cargo.lock
  radixdb-plugin-golden.toml
  install.sql
  src/
    lib.rs
```

The complete example is in `examples/public/rust-plugin`. The larger
`crates/radixdb-spatial` reference shows external point, box and polygon
types, scalar and batch functions, operators, a B-tree operator class and
bounded planner support.

## Minimal extension

```rust
use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "0199f8d2-7fb2-7c21-b5c1-6cb59c96b410",
    name = "radixdb_pair",
    version = "1.0.0"
)]
mod pair_plugin {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "pair",
        name = "pair",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 16,
        equality = pair_equal,
        hash = pair_hash,
        ordering = pair_compare
    )]
    struct Pair {
        #[radix_field(codec = "i64-le")]
        left: i64,
        #[radix_field(codec = "i64-le")]
        right: i64,
    }

    fn pair_equal(left: &Pair, right: &Pair) -> bool {
        left.left == right.left && left.right == right.right
    }

    fn pair_hash(value: &Pair, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.i64(value.left)?;
        sink.i64(value.right)
    }

    fn pair_compare(left: &Pair, right: &Pair) -> std::cmp::Ordering {
        (left.left, left.right).cmp(&(right.left, right.right))
    }

    #[radixdb_scalar(
        id = "pair_sum",
        name = "pair_sum",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    fn pair_sum(value: Pair) -> PluginResult<i64> {
        value
            .left
            .checked_add(value.right)
            .ok_or_else(|| PluginError::domain("pair sum overflow"))
    }
}
```

The outer module must be inline. The package UUID is permanent and independent
of its Cargo or SQL name. Every exported object has a stable local `id`; the
SDK derives its object identity from the package UUID and that ID. Keep both
unchanged for the lifetime of compatible persisted data.

## Macro map

| Macro | What the author declares | Why it is required |
| --- | --- | --- |
| `#[radixdb_plugin]` | Package UUID, canonical name and SemVer | Generates the only exported ABI entrypoint, immutable descriptor graph, negotiation and panic barriers |
| `#[derive(RadixType)]` with `#[radix_type]` | External type identity, codec and semantic revisions, storage shape, bounds and semantic callbacks | Separates persisted bytes from Rust memory layout and makes compatibility checks deterministic |
| `#[radix_field]` | Canonical little-endian field codec and sequence bounds | Prevents native-endian or unbounded serialization from becoming a durable format |
| `#[radixdb_scalar]` | Typed function signature and execution properties | Generates argument validation, typed conversion, result accounting, diagnostics and unwind containment |
| `#[radixdb_batch]` | Column callback paired with one scalar function | Accelerates hot paths while preserving the scalar function as the correctness contract |
| `#[radixdb_operator]` | SQL symbol, argument/result types and backing function | Gives the catalog an explicit, privilege-checked operator binding |
| `#[radixdb_operator_class]` | Core access method, input type, key type and key codec revision | Lets a plugin define semantics while RadixDB continues to own index storage and recovery |
| `#[radixdb_planner_support]` | Target function/class, result bounds and recheck policy | Supplies bounded candidate ranges without granting optimizer or storage ownership |

The exported `radixdb_aggregate`, `radixdb_window` and `radixdb_tvf` names are
reserved and deliberately fail compilation in 1.2. Native aggregate, window
and table-valued extension functions are outside this authoring revision.

## External types and codecs

`#[radix_type]` requires nonzero `codec` and `semantic_revision`, a `fixed` or
`variable` storage declaration, and `max_bytes` in `1..=16777216`. For a fixed
derived type, `max_bytes` must equal the encoded field width.

Derived codecs support signed and unsigned integers, `f32`, `f64`, `bool`,
fixed arrays and bounded `Vec<T>`. Primitive codecs are explicit
little-endian names such as `i64-le`, `u32-le`, `f64-le` and `bool-u8`.
A sequence must declare both `max_items` and `max_bytes`:

```rust
#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "sample_set",
    name = "sample_set",
    codec = 1,
    semantic_revision = 1,
    storage = "variable",
    max_bytes = 260
)]
struct SampleSet {
    #[radix_field(codec = "i64-le", max_items = 32, max_bytes = 260)]
    values: Vec<i64>,
}
```

Do not copy a Rust struct's memory as persisted bytes. Rust layout, padding and
native endian are not a storage contract. The float field codecs preserve
exact IEEE bits; they do not define how NaN or signed zero compare.

For a complex bounded shape, provide `manual = CodecType` and implement
`ManualCodec<T>` with `CodecReader`, `CodecWriter` and a deterministic corpus.
The reader must consume the complete canonical representation, and both paths
remain subject to `max_bytes`.

```rust
impl ManualCodec<Shape> for ShapeCodec {
    fn encode(value: &Shape, output: &mut CodecWriter) -> PluginResult<()> {
        if value.items.len() > 32 {
            return Err(PluginError::limit_exceeded("shape exceeds 32 items"));
        }
        output.write(&(value.items.len() as u32).to_le_bytes())?;
        for item in &value.items {
            output.write(&item.to_le_bytes())?;
        }
        Ok(())
    }

    fn decode(input: &mut CodecReader<'_>) -> PluginResult<Shape> {
        let bytes = input.read(4)?;
        let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if count > 32 {
            return Err(PluginError::limit_exceeded("shape exceeds 32 items"));
        }
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let bytes = input.read(8)?;
            items.push(i64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]));
        }
        Ok(Shape { items })
    }

    fn corpus() -> Vec<Shape> {
        vec![Shape { items: vec![] }, Shape { items: vec![0, i64::MAX] }]
    }
}
```

Declare equality, hash and ordering only when the type supports them. Hash
requires equality. The hash callback writes semantic components to
`HashSink`; it never chooses the engine's hash algorithm or physical token.
Equality-equivalent values must emit identical components, ordering must be
total and both relations must agree. The local test host checks these laws on
the generated or manual corpus.

Increase `codec` when canonical bytes change. Increase `semantic_revision`
when equality, hash, ordering, function, operator-class or planner semantics
change without changing bytes. A package SemVer change is not a substitute for
either revision.

## Scalar and batch functions

A scalar takes owned supported values and returns `PluginResult<T>`. Supported
built-ins include integer and floating Rust primitives, `bool`,
`BoundedText<N>` and `BoundedBytes<N>`; local `RadixType` values and
`Option<T>` express external types and nullability.

Each scalar declares exactly one of `immutable`, `stable` or `volatile`.
`strict` means NULL input produces NULL without entering the callback.
`parallel_safe`, `cost`, `cancellation = "bounded"` and the result byte bound
are execution promises used by the host. Declare them from behavior, not from
the desired plan.

An explicit batch adapter receives one `ColumnView` per scalar argument, a
typed `ColumnBuilder` and a `CallContext`. It must preserve scalar NULL,
ordering and error behavior and check cancellation at the declared interval:

```rust
#[radixdb_batch(for_scalar = "distance", rows_per_cancel_check = 64)]
fn distance_batch(
    left: ColumnView<'_, Point>,
    right: ColumnView<'_, Point>,
    output: &mut ColumnBuilder<'_, '_, f64>,
    context: &CallContext<'_>,
) -> PluginResult<()> {
    for (row, (left, right)) in left.zip(right).enumerate() {
        if row % 64 == 0 {
            context.check_cancelled()?;
        }
        output.push(distance(left?, right?)?)?;
    }
    Ok(())
}
```

The host publishes no partial batch after an error. The 1.2 scalar callback
shape does not expose `CallContext`, so scalar work must remain bounded by its
declaration. Batch callbacks use `check_cancelled()` and may use
`charge_work()` for additional budget accounting. Output must always remain
within the declared bound.

Return `PluginError::invalid_input`, `domain`, `limit_exceeded`, `cancelled`
or `internal` as appropriate. The host owns SQL diagnostic mapping. Generated
wrappers catch ordinary Rust unwinds, but cannot contain `abort`, segmentation
faults, undefined behavior or malicious code.

## Operators, operator classes and planner support

An operator is metadata over an already declared native scalar. The 1.2 Rust
macro declares binary operators with explicit left, right and result types.
An operator class then binds a complete strategy set and a canonical key
encoder to a core-owned B-tree, hash or bitmap index. B-tree requires `<`,
`<=`, `=`, `>=` and `>`; hash and bitmap require `=`. External HNSW operator
classes are not available in the 1.2 SDK.

```rust
#[radixdb_operator(
    id = "point_eq_operator",
    symbol = "=",
    semantic_revision = 1,
    function = "point_eq",
    left = Point,
    right = Point,
    result = bool
)]
fn point_eq_operator() {}

#[radixdb_operator_class(
    id = "point_morton_btree",
    semantic_revision = 1,
    access_method = "btree",
    input = Point,
    key = BoundedBytes::<16>,
    key_codec_revision = 1
)]
fn point_morton_key(point: Point) -> PluginResult<BoundedBytes<16>> {
    BoundedBytes::new(morton_bytes(point).to_vec())
}
```

Planner support reads a normalized predicate and emits bounded key spans. It
must set an estimate and declare exactly one of `exact` or `always_recheck`.
Use `always_recheck` for approximate covers; the executor evaluates the
original predicate for every candidate. The host canonicalizes and merges
spans, deduplicates rows and falls back conservatively when support cannot
produce a valid plan.

```rust
#[radixdb_planner_support(
    id = "within_box_support",
    name = "st_within_box_support",
    semantic_revision = 1,
    for_function = "within_box",
    operator_class = "point_morton_btree",
    always_recheck,
    max_spans = 1024,
    max_output_bytes = 49152
)]
fn within_box_support(
    predicate: PredicateView<'_>,
    output: &mut CandidatePlanBuilder<'_, '_>,
) -> PluginResult<()> {
    let bounds = predicate.constant::<Box2d>(1)?
        .ok_or_else(|| PluginError::domain("NULL bounds"))?;
    output.set_estimate(100, 10)?;
    for span in candidate_spans(bounds)? {
        output.push_span(span)?;
    }
    Ok(())
}
```

Neither macro grants access to WAL, pages, MVCC, compaction, transactions,
catalog mutation or physical index formats. If a capability requires such
ownership, it belongs in a generic engine API rather than a plugin callback.

## Tests, golden vectors and packaging

Use `radixdb_plugin::testing` in unit tests to validate descriptor graphs,
codec round trips, malformed bytes, scalar/batch parity, equality/hash/order
laws, operator-class strategies and planner spans. Every external type also
needs at least one canonical golden vector in
`radixdb-plugin-golden.toml`.

```toml
format = 1
package_id = "0199f8d2-7fb2-7c21-b5c1-6cb59c96b410"

[[types]]
object_id = "daa5e83d0ea33c36cade62862baeef54"
codec_version = 1
codec_fingerprint = "4899dde00ce9b0810701ad36df11a10910d4b46356295f7c8d2f5317effcc998"
vectors = [
    "00000000000000000000000000000000",
    "0100000000000000ffffffffffffffff",
    "0000000000000080ffffffffffffff7f",
]
```

Run the safe author loop from the RadixDB checkout:

```sh
cargo run --locked -p cargo-radixdb-plugin -- check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
cargo run --locked -p cargo-radixdb-plugin -- test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

`test-host` runs plugin tests, performs the official-shape build, inspects the
descriptor in an isolated child and validates all golden vectors. Package only
after those checks pass. The [tool reference](../../reference/programs/cargo-radixdb-plugin/)
describes official builds, compatibility comparison and artifact inspection;
the [administration chapter](../../administration/extensions/) covers server
installation and database binding.

The 1.2 SDK does not generate text input/output callbacks for SQL literals and
does not provide a generic high-level ORM representation for external values.
Clients exchange canonical bytes with protocol 17 and should generate a
plugin-aware adapter from `DESCRIBE DATABASE` metadata. Do not substitute raw
`BYTES`, because that loses type identity and codec admission.
