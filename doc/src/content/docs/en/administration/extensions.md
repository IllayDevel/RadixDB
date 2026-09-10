---
title: Installing Extensions
description: Install trusted native packages, bind them to a database and operate their lifecycle safely.
---

RadixDB 1.2 loads trusted native extension packages when the server starts.
The server operator installs a package on the host and explicitly allowlists
its absolute directory. A database owner then binds selected exports into a
database with SQL.

An extension runs inside `radixdb-server` with the privileges of that process.
It is not a sandbox. Install only packages whose source, build environment and
artifact provenance you trust.

## Package and database objects

The package and SQL objects have separate lifecycles:

1. A **package** is an admitted directory containing a shared library,
   manifest, codec vectors, compatibility report and build provenance.
2. An **extension binding** pins one exact package identity, version, ABI range
   and descriptor fingerprint in one database catalog.
3. External types, native functions, operators, operator classes and planner
   support are created explicitly from exports of that binding.

SQL never accepts a library path or URL and does not download code. A package
must already be present in the immutable process registry before
`CREATE EXTENSION` can succeed.

## Platform requirements

The first package platform is `x86_64-unknown-linux-gnu`. An admitted library
must be an x86-64 little-endian ELF shared object, require no glibc symbol newer
than 2.36, use plugin ABI 1.0 and carry the official `panic = "unwind"`
attestation. The official release tool builds packages with Rust 1.97.0 in
`rust:1.97.0-bookworm`.

The manifest is limited to 64 KiB and the shared library to 256 MiB. The
package directory, manifest, `lib` directory and library must be owned by root
or the effective server user and must not be group- or world-writable.
Symlinks are rejected.

## Place and allowlist a package

A complete package has this layout:

```text
radixdb-pair-1.0.0/
  radixdb-plugin.toml
  radixdb-plugin-golden.toml
  radixdb-plugin-provenance.toml
  radixdb-plugin-compatibility.toml
  lib/
    libradixdb_pair.so
```

Copy the complete directory without changing its files, then set restrictive
ownership and permissions. The destination itself must be an absolute,
normalized, non-symlink path.

```sh
sudo install -d -o root -g radixdb -m 0750 /opt/radixdb/plugins
sudo cp -a dist/radixdb-pair-1.0.0 /opt/radixdb/plugins/
sudo chown -R root:radixdb /opt/radixdb/plugins/radixdb-pair-1.0.0
sudo chmod -R go-w /opt/radixdb/plugins/radixdb-pair-1.0.0
```

Add a top-level `[plugins]` table to `server.toml`. Each entry names exactly
one package directory; the server does not scan parent directories.

```toml
[plugins]
package_directories = [
  "/opt/radixdb/plugins/radixdb-pair-1.0.0",
]
```

Restart the process. Plugins cannot be loaded, reloaded or unloaded in a
running process.

```sh
sudo systemctl restart radixdb
journalctl -u radixdb -n 100 --no-pager
```

Before opening the listener, the server validates every configured package as
one set: paths and permissions, manifest bounds, ELF target, SHA-256, ABI,
descriptor fingerprint and all descriptor references. Any failure aborts
startup; a partial registry is never published. A successful startup reports
registry generation and counts for packages, types, functions, operators,
operator classes and planner support.

`--print-endpoint` only parses the configuration and returns before package
admission. Use a real foreground or service start to validate package loading.

## Bind exports to a database

Run the package install script as the database owner or `root`. Keep related
objects in one transaction so that either the complete graph or none of it is
published.

```sql
BEGIN;
CREATE EXTENSION radixdb_pair VERSION '1.0.0';
CREATE TYPE public.pair FROM EXTENSION radixdb_pair AS 'pair';
CREATE FUNCTION public.pair_sum(value public.pair NOT NULL)
RETURNS INTEGER NOT NULL LANGUAGE NATIVE
FROM EXTENSION radixdb_pair AS 'pair_sum';
COMMIT;
```

The SQL names may differ from local export IDs. The local IDs in `AS` and the
package version are byte-exact stable identities; do not infer them from SQL
names. `IF NOT EXISTS` succeeds only for a byte-equivalent existing binding.

`DESCRIBE DATABASE` includes `radixdb.plugin_requirements.v1` in its descriptor
extensions. That object records each binding and external type identity,
codec version and payload limit. Use it to generate or validate client-side
adapters.

## Access and execution

Creating or dropping an extension requires the database owner or `root`.
Creating its dependent objects also requires ownership of the binding and
`CREATE` on the target schema. Native function calls use normal schema
visibility and Function `EXECUTE` privileges; operators check the backing
function privilege.

Authorization is completed before entering a plugin callback. A callback does
not receive the current principal, an ACL bypass, a catalog mutation handle or
storage internals.

External values cross protocol 17 as their stable type object ID, codec
version and bounded canonical bytes. They are not silently converted to
`BYTES`. A client that does not negotiate `ExternalValueV1` receives an
unsupported-type error.

## Version changes and removal

One server process selects only the highest configured SemVer for each package
UUID. Database bindings pin an exact version. Do not add a higher version to a
server that must continue opening databases bound to a lower version: the
lower artifact becomes shadowed and those databases enter restricted mode.

RadixDB 1.2 has no hot reload and no in-place `ALTER EXTENSION UPDATE` command.
Keep every exact package needed by a database and treat a version change as an
explicit application migration with a separately verified rollback. The
package tool can compare a new artifact with `--previous-package`, but a
compatible report does not change the catalog binding automatically.

Drop dependent objects in reverse order, then remove the binding:

```sql
BEGIN;
DROP FUNCTION public.pair_sum(public.pair) RESTRICT;
DROP TYPE public.pair RESTRICT;
DROP EXTENSION radixdb_pair RESTRICT;
COMMIT;
```

`CASCADE` is not supported for extension objects in 1.2. `RESTRICT` prevents
removal while a table, function, operator, index or other catalog object still
depends on the export.

## Missing or incompatible packages

If a database catalog requires a missing package, codec or semantic revision,
only that database enters restricted diagnostic mode. Ordinary SQL and
external-value decoding fail closed; other databases using the same server
remain independent.

Restore the exact admitted package and restart the server. If the binding has
no dependents, `root` may remove it with `DROP EXTENSION ... RESTRICT` while
the database is restricted. Do not edit catalog or data files to bypass the
check.

A physical database backup does not contain package directories. Preserve the
exact package artifacts, checksums and provenance beside backup records. The
bundled CLI backup and logical-export workflows do not yet accept a plugin
allowlist, so they are not a supported recovery path for an extension-bound
database. Establish and test a separate procedure before production use; see
[Backup and restore](../backup-restore/).

Continue with [native extension development](../../programming/native-extensions/)
or use the exact [extension SQL reference](../../reference/sql/extensions/).
