---
title: Backup and Restore
description: Create a consistent RadixDB 1.2 backup, keep it outside the database root, and restore it into a new directory.
---

A restartable database directory is not yet a backup. A backup must represent a
defined recovery point, survive loss of the source storage, carry an integrity
inventory, and be restored and checked before it is needed.

This chapter describes physical backup on documentation baseline
`23bf35df011aae6816d77578be96074b02bc363c`. The supported release procedure
uses `backup-external.sh` and `restore-external.sh` from the same verified
bundle as the CLI. It operates on one database root at a time and restores only
into a previously absent directory.

## Choose the artifact

| Artifact | Location and purpose | Recovery boundary |
| --- | --- | --- |
| Physical snapshot | `snapshots/<32-hex-id>/` inside a database root; local rollback and the source for an external backup | One catalog/data/index generation plus the required contiguous WAL suffix |
| External physical backup | Read-only copy of the retained snapshot tree, `BACKUP.env` and `SHA256SUMS` outside the database root | Same physical format; restore with the matching release first |
| Logical SQL dump | Versioned SQL stream created by `--export-sql` | Schema and rows for migration to a release that does not read the old physical format |

An internal snapshot remains on the same filesystem as the database. It does
not survive loss of that filesystem and is not an independent backup. A
checkpoint is also not a backup: it publishes a new storage generation and
advances WAL retention, but it does not create an external recovery artifact.

Do not copy a live database root with `cp`, `rsync` or a filesystem archiver.
The copied CONTROL, manifests, artifacts and WAL can belong to different
publication boundaries. Use the engine snapshot path or a storage snapshot
procedure that has been separately integrated and tested with RadixDB.

### Databases with native extensions

The bundled CLI and `backup-external.sh` do not currently accept a native
package allowlist. They therefore open with an empty plugin registry and cannot
create or validate a backup of a database bound to an extension. The same
restriction applies to the documented CLI logical export/import procedure.

Do not treat these wrappers as recovery evidence for extension-bound data.
Preserve the exact complete packages separately and establish an independently
tested backup and restore procedure before production use. Copying live files
or disabling exact package admission is not a substitute. The ordinary
procedure below applies to databases with no extension binding.

## Snapshot consistency

`PRAGMA SNAPSHOT` is valid only for a persistent database and outside an
explicit transaction:

```sql
PRAGMA SNAPSHOT;
```

The engine briefly freezes one commit boundary in checkpoint lock order, pins
the published physical generation and freezes every WAL generation from its
replay floor. It then copies reachable catalog, table manifests, DATA and INDEX
artifacts, and the required WAL suffix. Each member has a declared length and
SHA-256 digest. `SNAPSHOT.mft` is synchronized and published last; a directory
without that final manifest is not a committed snapshot.

Normal commits can continue after the boundary has been frozen while the
snapshot copies immutable members. DDL, checkpoint and compaction remain fenced
for longer. Cancellation returns an error and incomplete snapshot work is not a
valid recovery point.

An explicit checkpoint before the snapshot is not required for consistency.
Committed hot rows after the replay floor are carried by WAL. A checkpoint can
reduce that WAL suffix and make backup size and restore work more predictable,
but it adds its own I/O and publication cycle.

## Prepare an external backup

The packaged script opens the file database through `radixdb-cli`, so the
database's exclusive `LOCK` must be available. For a server installation, stop
the whole service cleanly. This prevents an unopened database from being opened
by a new client while the backup is running.

Retain the complete checksum-verified release bundle: the systemd installer
copies the CLI into `/opt/radixdb/bin`, but does not install the backup scripts.
The backup parent must already exist, be writable by `radixdb`, and reside
outside the database root. Ensure space for a new internal snapshot and its
external copy; there is no fixed size ratio because indexes and the WAL suffix
vary by workload.

```sh
BUNDLE=/srv/radixdb-releases/1.2
DATABASE_ROOT=/opt/radixdb/data/databases/app
BACKUP_PARENT=/srv/radixdb-backups
BACKUP="$BACKUP_PARENT/app-$(date -u +%Y%m%dT%H%M%SZ)"

sudo install -d -o radixdb -g radixdb -m 0700 "$BACKUP_PARENT"
sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
sudo -u radixdb "$BUNDLE/backup-external.sh" "$DATABASE_ROOT" "$BACKUP"
sudo systemctl start radixdb
```

Run the command once for every named database that requires a recovery point.
The target must not already exist. The script rejects a destination below the
source database, creates a physical snapshot, captures its exact 32-hex identity,
rejects symbolic links, copies only that committed snapshot, records an inventory
and removes write bits only after checksum verification succeeds. An interrupted
script removes its incomplete destination.

## Verify and retain the backup

Do not treat the final success line as the only evidence. Verify the inventory
again after transfer and regularly on retained media:

```sh
(cd "$BACKUP" && sha256sum -c SHA256SUMS)
find "$BACKUP" -perm /222 -print
```

The second command should print nothing. Read-only mode is protection against
accidental writes, not encryption or authentication. The snapshot contains
database contents in engine format. Restrict access to the backup location and
protect a copy of `SHA256SUMS` separately; anyone able to replace both data and
the checksum file can create a new internally consistent inventory.

Keep the release bundle and its `PROVENANCE.env` beside, but not inside, the
immutable backup. Checksum-covered `BACKUP.env` records external-backup format,
creation time, CLI version, git revision, build profile and target, Cargo.lock
digest, physical format, database identity and exact snapshot ID. Restore checks
all fields before creating a target and rejects an incompatible physical format.

## Restore into a new root

Never test a restore over the only source copy. The release wrapper requires an
absent target, verifies the complete external inventory before creating it,
rejects symbolic links, copies snapshots, and asks the engine to restore a
validated generation. Failed work is removed instead of becoming a selectable
database.

Use a valid server database name for the new directory: ASCII letters, digits,
`_` and `-` only.

```sh
BUNDLE=/srv/radixdb-releases/1.2
BACKUP=/srv/radixdb-backups/app-20260908T080000Z
RESTORED=/opt/radixdb/data/databases/app-restore-20260908

sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
test ! -e "$RESTORED"
sudo -u radixdb "$BUNDLE/restore-external.sh" "$BACKUP" "$RESTORED"
```

The restored root receives new runtime directories and CONTROL publication only
after manifest, member, checksum, database identity, reachability and WAL-range
validation. The source backup remains read-only. Index artifacts included by
the 1.2 snapshot are restored; a snapshot format that explicitly omits a
rebuildable index must still pass the engine's rebuild-state checks.

## Validate before cutover

While the server remains stopped, open the new root with the CLI from the
matching bundle. Check application invariants, not only a row count. Include
primary and unique constraints, secondary-index queries, views, recent rows
expected from WAL, and representative aggregates.

```sh
sudo -u radixdb "$BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$RESTORED" \
  --execute "SELECT COUNT(*) AS rows, MIN(id) AS first_id, MAX(id) AS last_id FROM items"
sudo -u radixdb "$BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$RESTORED" --execute "SHOW INDEXES FROM items"
```

Close the CLI, start the service and select the restored database by its new
name. Wait for `database_status(name)` to report ready and repeat an application
read. Keep the old root unchanged until the restored database has passed the
acceptance window.

```sh
sudo systemctl start radixdb
sudo systemctl status radixdb --no-pager
journalctl -u radixdb -n 100 --no-pager
```

The external wrapper restores the exact checksum-covered `snapshot_id` from
`BACKUP.env`; it never selects by wall-clock order. It also checks that the
copied manifest reports the same snapshot, database and physical-format
identities, and fails instead of falling back when that selected snapshot is
missing or corrupt.

## Local restore

`PRAGMA RESTORE` and CLI `--restore` replace the current database generation.
They are destructive rollback tools, not the preferred disaster-recovery test.
Without an ID, the latest valid committed snapshot is selected. A specific ID
is exactly 32 lowercase hexadecimal characters and is visible as its snapshot
directory name:

```sh
find "$DATABASE_ROOT/snapshots" -mindepth 1 -maxdepth 1 -type d -printf '%f\n'
radixdb-cli --db "file://$DATABASE_ROOT" \
  --restore 0123456789abcdef0123456789abcdef
```

Restore is rejected inside an explicit transaction. It stops new transaction
admission, waits up to five seconds for active transactions, stages and
validates the selected generation, then performs a journaled component swap.
The process can continue after success, but a production rollback still needs
exclusive operational control and post-restore validation.

CLI help uses the same 32-hex identity format and describes snapshot retention
at database scope.

## Physical compatibility and logical migration

The external wrapper format does not promise that a later engine can read an
older physical generation. First prove recovery with the matching archived
binary. For migration between physical formats, export through the old CLI and
import into an absent root through the new CLI:

```sh
old/radixdb-cli --db file:///srv/radixdb-old --export-sql database.sql
new/radixdb-cli --db file:///srv/radixdb-new --import-sql database.sql
```

Logical export uses one MVCC snapshot and an atomic output file. Import verifies
the stream and builds a full-sync sibling database before publishing the absent
target. It does not replace independent physical backups, and the upgrading
chapter defines the complete version transition.

## Failure response

| Observation | Required action |
| --- | --- |
| Source lock cannot be acquired | Confirm the server and every embedded owner are stopped; do not remove `LOCK` |
| Snapshot or checksum creation fails | Keep the source unchanged, remove only the incomplete external destination, and repair capacity or I/O |
| `sha256sum -c` fails after transfer | Quarantine the artifact; do not restore it or rewrite its inventory |
| Restore target already exists | Choose another absent root; do not delete a database to satisfy the wrapper |
| Restore rejects a member, identity or WAL range | Preserve logs, backup and matching binary for diagnosis; do not copy individual files around the error |
| Restored data differs from the recorded recovery point | Do not cut over; retain both roots and investigate snapshot selection and application invariants |

Test restoration on a schedule and after every release, storage change and
backup-destination change. Record source build identity, database name, backup
path, expected data fingerprint, checksum result, restore duration and the
identity of the binary that reopened the result.

See [storage architecture](../storage/) for generation layout and
[server operation](../server/) for clean shutdown and database readiness.
