---
title: Troubleshooting
description: Diagnose RadixDB startup, disk, I/O and recovery failures while preserving evidence and recovery options.
---

Troubleshooting begins by preserving the first failure and stopping additional
state changes. A restart can recover an ordinary process crash, but repeated
automatic restarts can overwrite useful logs and repeatedly exercise failing
storage.

Do not manually delete or replace `LOCK`, CONTROL, WAL, catalogs, manifests,
DATA, INDEX, snapshots, staging or quarantine files. They participate in
ownership, reachability and atomic publication. Removing one artifact can turn
a diagnosable failure into permanent loss.

## First response

Stop application traffic. If the service is restarting or reports I/O or
corruption, stop the unit before collecting evidence:

```sh
sudo systemctl stop radixdb
systemctl status radixdb --no-pager
journalctl -u radixdb --since '30 minutes ago' --no-pager
radixdb-server --version
sha256sum /opt/radixdb/bin/radixdb-server /opt/radixdb/bin/radixdb-cli
```

Record the exact time, last acknowledged application operation, configured
endpoint, database name, build identity, configuration checksum, filesystem
mount and the first error. Preserve an external byte-for-byte copy or storage
snapshot before repair when the device is readable. Do not write that copy
inside the affected database filesystem.

## Classify the failure

| Observation | Likely boundary | Next action |
| --- | --- | --- |
| Unit is inactive before listener | Configuration, permissions, bind or process failure | Read the first journal error; validate paths and endpoint |
| Listener works, named database is `Starting` | Database has not been selected | Select it and wait for named readiness |
| Named database is `Opening` or recovering | Strict open and WAL replay are in progress | Wait with a bounded deadline; watch journal and host I/O |
| Named database is `Emergency` | Open/recovery/close failed | Stop restart loops, preserve exact message and storage copy |
| `disk_reserve_exhausted` | Compaction cannot preserve its configured free-space reserve | Add capacity or move unrelated data outside the database tree |
| `ENOSPC` or no free inodes | Filesystem can no longer accept required writes | Stop writes, restore capacity outside the database tree, then validate |
| `failed to write to WAL` or `failed to sync WAL` | Write path, device, mount or filesystem I/O failure | Treat commit outcome from a lost connection as uncertain; inspect OS errors |
| Required DATA/catalog/manifest is missing or corrupt | Media or artifact loss, not an ordinary crash | Restore a verified external backup into a new root |
| Optional index is unavailable | Rebuildable accelerator failed validation | Preserve diagnosis; allow only the documented scan fallback and rebuild path |

## Disk exhaustion

Check both bytes and inodes on the database mount. Identify growth without
deleting engine-owned files:

```sh
DATA=/opt/radixdb/data
df -B1 "$DATA"
df -i "$DATA"
du -x --max-depth=2 --block-size=1 "$DATA" | sort -n
journalctl -u radixdb -g 'ENOSPC|disk_reserve_exhausted|backpressure|checkpoint|WAL' --no-pager
```

Compaction needs room for new immutable output while old inputs remain
reachable. The engine therefore reserves free space beyond the output budget.
When the reserve check fails, the exact compaction input enters a retry
cooldown. Writes may continue until L0 reaches hard backpressure limits; do not
wait for that rejection before adding capacity.

Actual `ENOSPC` is different. A WAL append, sync, checkpoint or snapshot can
return an error. The verified ENOSPC failpoint rejects the affected insert
without publishing its row and the database remains reopenable, but that test
does not promise that every real device failure has the same scope. Stop new
writes, free or extend capacity outside the database root, verify the device,
then reopen with the same binary and check application invariants.

## I/O and durability failures

Inspect the mount and kernel before blaming SQL:

```sh
findmnt -T /opt/radixdb/data -o TARGET,SOURCE,FSTYPE,OPTIONS
journalctl -k -p warning..alert --since '30 minutes ago' --no-pager
cat /proc/mounts
```

A write error reaching the client is a failed operation. If the connection is
lost before a commit reply, the client may not know whether the commit became
durable. Do not blindly retry non-idempotent writes; reconnect, query by an
application idempotency key or transaction identity, and reconcile first.

`sync_mode=normal` synchronizes commit and DDL durability boundaries;
`sync_mode=full` synchronizes every WAL write. `sync_mode=none` does not force
WAL synchronization, so acknowledged work before a durable checkpoint has a
weaker power-loss guarantee. No mode protects against loss of the storage
device itself. See [configuration](../configuration/) for the deployed value.

After an I/O error, do not resume merely because one write succeeds. Check the
filesystem, controller and kernel log, then run a controlled restart and an
application read/write/rollback probe. Repeated sync failures require taking
the database out of service and restoring or relocating it.

## Crash versus media loss

| Event | What RadixDB can recover from | What remains outside the guarantee |
| --- | --- | --- |
| Clean stop | Closed engine state plus durable checkpoint/WAL | Hardware loss after shutdown still requires a backup |
| Process crash, `SIGKILL` or host reset with intact durable storage | Select newest complete CONTROL generation and replay its contiguous committed WAL suffix | Acknowledgements weaker than the configured sync mode; external side effects |
| Power loss | Same strict recovery from bytes the storage actually made durable | Volatile device caches and `sync_mode=none` acknowledgements |
| Missing or corrupt required artifact | A previous complete CONTROL candidate may be selected if its entire reachable graph and WAL are valid | WAL does not replace missing committed DATA/catalog/manifest |
| Lost filesystem or device | Nothing from that device | Restore an independent external backup on healthy storage |

Normal crash recovery is an open of the existing root with the matching binary.
It validates both CONTROL slots, chooses only a complete reachable generation,
and replays committed WAL in order. Malformed or incomplete records do not
publish partial transactions. A missing optional index can be represented as
unavailable; missing required DATA invalidates the candidate.

Media recovery is different. Restore a checksum-verified external backup into
an absent root on healthy storage. Do not create a replacement root by copying
surviving individual files around a validation error. If no complete CONTROL
candidate exists, fail-closed open is the correct outcome.

## Controlled recovery

1. Keep the failed root and evidence unchanged.
2. Verify host storage and capacity; repair the platform before exercising the
   database again.
3. Use the exact source bundle to attempt one controlled open of a copy or
   storage snapshot. Capture the complete error.
4. If strict recovery succeeds, verify rows, constraints, indexes, views and
   recent committed application facts before returning traffic.
5. If required artifacts are lost or strict open fails, restore a verified
   external backup into a new absent root and validate it before cutover.
6. For an incompatible target release, recover with the source binary first,
   then follow the logical [upgrade procedure](../upgrading/).

`--reset-storage` and local `PRAGMA RESTORE` are destructive recovery tools,
not general fixes for corruption. They can replace or quarantine the current
generation and require exclusive ownership. Use them only under their explicit
runbook with a preserved copy and a known recovery point.

## Return to service

Start the server once, verify its build identity, select the database, require
named ready status with a complete artifact scan, and execute representative
application checks. Compare the result to the recorded recovery point. Keep
the previous root and logs through an observation window.

RadixDB 1.2 does not claim high availability, replication, automatic failover,
point-in-time recovery to an arbitrary WAL position, or recovery from lost
media without an external backup. Operational procedures must not turn strict
failure into a silent fallback.

See [backup and restore](../backup-restore/) for disaster recovery and
[monitoring](../monitoring/) for the signals used during diagnosis.
