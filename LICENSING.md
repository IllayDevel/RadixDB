# RadixDB licensing

Copyright (c) 2026 RadixDB contributors.

RadixDB uses component-based licensing. This file defines the license boundary
for the repository. A license notice in a component directory takes precedence
for that component. Third-party dependencies and vendored files remain under
their respective licenses.

## PolyForm Perimeter 1.0.1 components

The following are licensed under the
[PolyForm Perimeter License 1.0.1](LICENSES/PolyForm-Perimeter-1.0.1.txt):

- the root `radixdb` package and all binary targets currently defined by it,
  including the embedded engine, server, and `radixdb-cli`;
- `radixdb-api`;
- `radixdb-catalog`;
- `radixdb-core`;
- `radixdb-executor`;
- `radixdb-functions`;
- `radixdb-plugin-host`;
- `radixdb-procedural`;
- `radixdb-sql`;
- `radixdb-storage`;
- internal workload and soak packages `radixdb-join-workload` and
  `radixdb-soak`;
- all other repository content not explicitly designated below.

The Perimeter license permits use, modification, and distribution for permitted
purposes, but does not permit providing others with a product that competes
with RadixDB.

## Apache 2.0 components

The following are licensed under the
[Apache License 2.0](LICENSES/Apache-2.0.txt):

- `crates/cargo-radixdb-plugin`;
- `crates/radixdb-client`;
- `crates/radixdb-orm`;
- `crates/radixdb-plugin`;
- `crates/radixdb-plugin-abi`;
- `crates/radixdb-plugin-macros`;
- `crates/radixdb-protocol`, including its public wire-protocol definitions;
- `crates/radixdb-spatial`, the reference native extension;
- `crates/radixdb-plugin/tests/fixtures/proof-plugin`;
- public Rust client, ORM, and extension examples under `examples/public`.

Using an Apache-licensed component does not change the license of a
Perimeter-licensed engine component linked or distributed with it.

## Command-line client boundary

The current `radixdb-cli` target is part of the root engine package and is
therefore licensed under PolyForm Perimeter 1.0.1. A future client-only CLI
package may be released under Apache 2.0 once it is structurally separated from
the embedded engine.

## Commercial licensing

A separate written commercial license is required for permissions outside the
Perimeter grant, including authorized competing products, rebranding, OEM
database distribution, and competing hosted or managed database services.

Commercial licensing steward: **Ledenev Nikita**.

Contact: **dev@radixdb.org**.

## Contributions

Contributors retain copyright in their contributions. Contributions are
accepted under the component license and the
[RadixDB Contributor License Agreement](CLA.md), which grants the licensing
steward the rights needed to maintain this component model and offer commercial
licenses.

## Trademarks

Copyright licenses do not grant rights to use RadixDB names or logos except as
needed for accurate attribution. See [TRADEMARKS.md](TRADEMARKS.md).
