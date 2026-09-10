---
title: Upgrading RadixDB
description: Move a database to a new RadixDB release without treating physical files as a compatibility interface.
---

An upgrade changes more than an executable. The server wire protocol, SQL
behavior, configuration schema, physical storage format and logical dump format
have separate compatibility boundaries. Check each boundary against the target
release before changing a production service.

This procedure is verified against documentation baseline
`40b1b3d13e050afa2666a0414b7215d5ac1452c0`. The baseline uses wire protocol
17 and a strict V6 physical reader. It has no in-place physical migration and
does not open retired storage generations through compatibility defaults.

## Compatibility boundaries

| Boundary | Upgrade rule |
| --- | --- |
| Server and client | Build identity and wire protocol must be compatible; protocol 17 rejects incompatible handshakes |
| SQL and application | Run application queries and invariants against the candidate; syntax acceptance alone is not compatibility proof |
| Configuration | Validate the complete candidate `server.toml`; unknown and invalid values fail startup, and changes require restart |
| Native extensions | Preserve every exact package, fingerprint and codec required by each database; packages are outside physical backup |
| Physical database | Open only with a release that explicitly accepts that format; there is no in-place converter in this baseline |
| Logical SQL dump | Export with the old binary, import with the new binary into an absent root, then validate and re-export |
| Backup | First prove that the archived source binary can restore and open it; a backup is not automatically forward-compatible |

Do not replace only the server while leaving an unverified client, or replace
only the CLI used for recovery. Keep each checksum-verified release bundle with
its build provenance, configuration template, CLI and recovery scripts.

## Native extension boundary

Before replacing the server, inventory the extension bindings reported by
`DESCRIBE DATABASE` and archive each matching complete package. Adding a higher
SemVer with the same package UUID makes that version active at the next startup;
it does not update an existing catalog binding. A database pinned to the old
version then opens in restricted diagnostic mode if the old exact package is no
longer active.

RadixDB 1.2 has no `ALTER EXTENSION UPDATE`, hot reload or automatic codec
migration. Keep the old package available for rollback and perform an explicit
logical or application migration into separately verified objects when an
extension changes identity or codec. The compatibility report emitted by
`cargo radixdb-plugin package --previous-package` is release evidence, not a
catalog migration. See [extensions](../extensions/).

## Prepare the transition

Record both identities, the paths involved and free space before downtime. Use
absolute paths. The old root and new root must be different, and the new root
must not exist.

```sh
OLD_BUNDLE=/srv/radixdb-releases/old
NEW_BUNDLE=/srv/radixdb-releases/1.2
OLD_ROOT=/opt/radixdb/data/databases/app
NEW_ROOT=/opt/radixdb/data/databases/app-v11
DUMP=/srv/radixdb-migrations/app-v11.sql

"$OLD_BUNDLE/bin/radixdb-cli" --version
"$NEW_BUNDLE/bin/radixdb-cli" --version
test -d "$OLD_ROOT"
test ! -e "$NEW_ROOT"
df -B1 "$OLD_ROOT" "$(dirname "$NEW_ROOT")" "$(dirname "$DUMP")"
```

Before the maintenance window:

1. Test an external physical restore with the old bundle.
2. Rehearse logical export/import on a copy and record row, constraint, index,
   view and application fingerprints.
3. Validate the candidate configuration and client protocol in an isolated
   service.
4. Estimate space for the dump, the new physical root and rollback retention.
5. Define an application write stop. RadixDB 1.2 does not provide replication,
   online logical catch-up or automatic failover for this transition.

## Export through the old engine

Stop application writes and the server. Confirm that no embedded process owns
the database lock. Export through the old CLI, because it is the authoritative
reader for the old physical files.

```sh
sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
test ! -e "$DUMP"
sudo -u radixdb "$OLD_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$OLD_ROOT?checkpoint_on_close=off" \
  --export-sql "$DUMP"
sha256sum "$DUMP" > "$DUMP.sha256"
```

The dump is a versioned deterministic SQL stream with source identity, schema
fingerprint, counts and a checksum trailer. Export reads one MVCC snapshot and
publishes the output atomically. Keep the old physical root unchanged and
offline; it is the rollback source until acceptance finishes.

Acquiring exclusive ownership refreshes the `LOCK` owner record. The verified
export leaves the remaining durable source artifacts byte-identical; therefore
compare source data and reachable artifacts, not the transient lock record.

The smaller logical migration oracle, executable documentation roundtrip and
messenger-shaped rehearsal pass. The latter now produces three byte-identical
exports, verifies a reviewed logical-content digest after normalizing only the
source-version provenance line, deletes the source root, imports into an absent
target, checks all 18 tables, 8 views and 37 rows, then reopens and re-exports
the target. This still does not replace rehearsal of the exact source binary,
target binary and application shape for a real cross-version transition.

Never copy old CONTROL, WAL, catalog, manifests, DATA, INDEX, snapshots or
retired volumes into a target root. WAL is recovery material for its own
physical generation, not a cross-version transport.

## Import into an absent root

The new CLI verifies the stream, creates a full-sync sibling staging database,
executes the dump, checkpoints and closes it, and only then publishes the target
with `RENAME_NOREPLACE`.

```sh
sha256sum -c "$DUMP.sha256"
test ! -e "$NEW_ROOT"
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$NEW_ROOT" \
  --import-sql "$DUMP"
test -d "$NEW_ROOT"
```

A checksum, SQL, checkpoint or close failure leaves the requested target
absent. An existing target is rejected and must not be emptied to make the
command succeed. Preserve the failed dump and logs; choose a fresh target for
the next attempt.

## Validate before cutover

First query the new root offline with the new CLI. Verify more than counts:
primary and unique constraints, foreign-key behavior, secondary-index plans,
views, NULL values, recent committed rows and application aggregates.

```sh
REEXPORT=/srv/radixdb-migrations/app-v11.reexport.sql
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$NEW_ROOT?checkpoint_on_close=off" \
  --execute "SELECT COUNT(*) AS rows FROM critical_table"
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$NEW_ROOT?checkpoint_on_close=off" \
  --export-sql "$REEXPORT"
cmp --silent "$DUMP" "$REEXPORT"
```

An exact re-export is a strong transport check for releases that share the dump
contract, but it does not replace application tests. If the target release
documents a new logical dump revision, compare declared counts and canonical
application fingerprints instead of assuming byte equality.

Point an isolated candidate server at a data directory containing the new root
under its final database name. Select it, wait for `database_status(name)` to
be ready, and run a representative read/write/rollback probe. Then switch the
application and monitor errors, latency, WAL, compaction and disk usage through
an acceptance window.

## Rollback boundary

Before the first new-version write, rollback can select the untouched old root
and old bundle. After new writes begin, switching back discards those writes
unless the application has a separately tested reconciliation path. The old
binary must never open the new physical root merely to attempt rollback.

Keep the old root, dump, checksums, old bundle and pre-upgrade external backup
until acceptance and rollback retention have expired. A successful import is
not a reason to delete the only recoverable old-format copy.

See [backup and restore](../backup-restore/) before rehearsing the transition,
and [monitoring](../monitoring/) for candidate acceptance signals.
