# CA-90.3 six-hour acceptance report

[Русский](CA_90_3_6H_ACCEPTANCE_REPORT.ru.md)

Status: **ACCEPTED / PASS**.

Date: 2026-09-07 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Automated terminal result: `passed`.

CA-90.3 is accepted as a successful strengthened acceptance run. The observed
storage starvation, ATA reset and temporary watchdog stalls are part of the
evidence rather than grounds for revoking PASS: the engine recovered progress
each time and completed the full checkpoint, snapshot, restore and digest
oracle without an invariant failure.

## 1. Immutable identity

| Parameter | Value |
|---|---|
| Engine/soak Git SHA | `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |
| Version | `radixdb-server/radixdb-soak 0.5.2` |
| Protocol | `14` |
| Build profile / target | `release / x86_64-unknown-linux-gnu` |
| `Cargo.lock` SHA-256 | `dadf5eee3abdf18536a1a23c7a86553b658a0c450c5c5bd87a10a9c0c8af2f2d` |
| Config SHA-256 | `1edf165c21651f9888b6f5257f62bb28f0fc29e5da524e7ea61330898f91e025` |
| Server config SHA-256 | `2cca6d86627f452f905a370a0d9d6d4aa77f648849917c387c24b36df830aada` |
| Manifest SHA-256 | `4cc2d80343aac1b40a520d22b8ad753b8de068a8458bed6e9b9f66effbbc9862` |
| Final `REPORT.json` SHA-256 | `9ae80e5ab496caa8dfe9beff83d224a0b0ce19226777c7c4667a27fb2f6da715` |
| Final `status.json` SHA-256 | `3c5ded5539513c2b863efdebf15273e1f8522e7320ab1daa4333c7882e42216c` |

A newer documentation or evidence commit is not the identity of the binary
that ran and does not replace the SHA in this table.

## 2. Frozen profile

| Parameter | Value |
|---|---:|
| Mode / profile | `full / 6h` |
| Workload duration | `21 600 000 ms` |
| Seed | `1 592 594 944` |
| Active rows | `100 000 000` |
| Client ladder | `16, 32, 64, 128, 256` |
| Checkpoint interval | `5m` |
| Invariant interval | `30s` |
| Graceful reopen | `2h` |
| SIGKILL/reopen | `4h` |
| Recovery timeout | `20m` |
| Effective watchdog | `21m` |

Chunk and job geometry, I/O budgets, L0 thresholds and worker count were not
tuned to the actual HDD.

## 3. Hardware and storage

| Parameter | Value |
|---|---|
| CPU | Intel Celeron 847, 1.10 GHz |
| Logical CPUs | `2` |
| RAM | `1 843 860 KiB`, approximately `1.76 GiB` |
| Storage | Toshiba MQ01ABD050, 5400 rpm HDD |
| Filesystem | `/dev/sda1`, ext4 `rw,relatime,errors=remount-ro` |
| Degraded transport | `1.5 Gbit/s + UDMA/33` |
| Surface counters | reallocated/pending/offline-uncorrectable `0/0/0` |
| Transport history | SMART UDMA CRC `3`; live kernel ATA bus error |

The complete device record and interpretation boundary are in the
[`CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md`](CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md).

## 4. Terminal result

The complete wall time, including seed and final evidence gates, was
`27 427 411 ms`, or `7h 37m 07.411s`.

| Counter | Result |
|---|---:|
| Transactions planned | `128 652` |
| Transactions committed | `98 668` |
| Transactions rolled back | `14 259` |
| Conflicts | `8 604` |
| Operations | `2 351 035` |
| Ambiguous outcomes resolved | `160` |
| Graceful/SIGKILL reopens | `2` |
| Successful checkpoints | `2` |
| Deferred checkpoints | `51` |
| Final snapshots | `1` |
| Final restores | `1` |
| Invariant passes / failures | `2 100 / 0` |
| Telemetry drops | `0` |
| Terminal failure | none |

All 12 invariant families passed 175 checks each. In particular:

- `cold_fixture_cardinality`: `expected=100000000 actual=100000000`;
- `account_balance`: `balances=6842419 postings=6842419`;
- `view_cardinality`: `base=31727 view=31727`;
- `dependent_view_cardinality`: `base=31727 dependent_view=31727`;
- `navigation_classic_parity`: `classic=31727 navigation=31727`;
- graph/revision/master-detail invariants: `violating_rows=0`;
- snapshot repeatable read: `start=31727 end=31727`.

After `final-checkpoint`, the run successfully performed:

1. source `final-invariants`;
2. `final-snapshot` publication;
3. independent copy to `final-snapshot-evidence`;
4. `final-restore` into a separate database root;
5. `final-restore-invariants`;
6. source/restore `final-digest` comparison.

Final logical digest:

```text
59b56e6b7bdaf846dd167aa01185222c7af95aec54593b9a05927b4c0abda4b0
```

The agent unit exited normally with `Result=success`, `ExecMainStatus=0`.

## 5. Observed complications

### 5.1 First side-I/O episode

Between 16:39:16 and 16:46:21 +07, random data was written directly to the
same filesystem:

- duration `7m 05.471s`;
- `13 883 146 240` bytes written;
- the episode overlapped the scheduled SIGKILL/reopen;
- reopen completed in `23 043 ms`;
- all 12 immediate recovery invariants passed.

### 5.2 Bounded 15-minute pressure

Between 17:06:05 and 17:21:05 +07, the preserved
[`side-io-pressure.sh`](../../../../crates/radixdb-soak/deploy/atom-hdd/side-io-pressure.sh)
was applied:

- 900 seconds of direct random writes;
- `29 657 923 584` bytes written;
- another 300 seconds reserved for recovery observation;
- progress resumed while the direct competing load was still active.

### 5.3 Real hardware incident

At 17:26:31 the kernel recorded:

```text
failed command: FLUSH CACHE EXT
Emask 0x10 (ATA bus error)
hard resetting link
SATA link up 1.5 Gbps
configured for UDMA/33
retrying FLUSH 0xea Emask 0x10
EH complete
```

This was a physical transport failure, not a failpoint or device-mapper
injection. The kernel restored the link and retried the flush; there was no
permanent `EIO`, ext4 error or read-only remount.

The full timeline and raw excerpts are in the
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md).

### 5.4 Two watchdog stalls

At `clients-256`, the accumulated maintenance tail twice pushed semantic
silence beyond the 21-minute watchdog corridor. The run remained alive and
returned to `healthy` without intervention both times:

```text
85 770 -> 91 021 -> 95 361 -> 98 668 commits
```

After the final recovery, the engine completed the worker phase, final
checkpoint, snapshot, restore, restored invariants and digest. The intermediate
`watchdog=stalled` state is therefore evidence of a prolonged-latency episode,
not a terminal liveness failure.

## 6. Resources

| Metric | Value |
|---|---:|
| Peak server RSS from `samples.jsonl` | `1 060 020 224` bytes, approximately `1 010.9 MiB` |
| Final server RSS | `206 327 808` bytes, approximately `196.8 MiB` |
| Minimum observed RSS | `148 299 776` bytes, approximately `141.4 MiB` |
| Final database tree | `3 670 799 691` bytes |
| DATA | `3 349 117 547` bytes |
| INDEX | `193 301 356` bytes |
| Metadata | `9 963 824` bytes |
| WAL | `118 415 802` bytes |
| Final threads / open FDs | `8 / 44` |

The final tree includes the source and a separate restored database, so it
must not be compared directly with the size of one active database shown on
the dashboard.

After the 256-client workload ended, RSS returned to approximately
`180–200 MiB`. Memory used by the active stage was released at quiescence; the
terminal evidence does not show the gigabyte peak persisting as a permanent
tail.

Latency and throughput in this run are not a product SLA: they were
deliberately affected by a degraded HDD, two side-load episodes and a real ATA
reset.

## 7. Observer verdict

The observer reached terminal state after the agent completed:

| Metric | Value |
|---|---:|
| Terminal | `true` |
| Samples | `14 176` |
| Telemetry sequence | `7 166` |
| Telemetry gaps | `0` |
| Retention dropped records | `0` |
| Retention saturated | `false` |
| Last error | none |

The confirmed `disk_degradation-13014` correctly remained active with
`severity=fatal`: the physical storage path did not become healthy merely
because the link came back. The corresponding `incident-000104` remains open
and has no `engine-after.json` or `threads-after.json`, because the persistent
hardware alert never received a clean recovery edge. This limitation remains
explicit in the result: the kernel log, before snapshot, burst telemetry and
terminal engine evidence are preserved, while database state is independently
confirmed by the complete restore/digest oracle.

## 8. Evidence location

The complete raw bundle is stored outside the public Git repository. Its local
benchmark-host path is intentionally omitted from this published copy. The
bundle contains:

- `manifest.json`;
- `status.json`;
- `REPORT.json` and `REPORT.md`;
- `events.jsonl`, `samples.jsonl`, `semantic-progress.jsonl`;
- observer/engine/host/disk/process/kernel/SMART telemetry;
- `incidents/000104-disk-degradation/`;
- `backups/final-snapshot/`.

The selected incident log is included in the public evidence collection:
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log).

## 9. Acceptance boundary

CA-90.3 is accepted as PASS for the tested binary
`dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

Acceptance means:

- correctness and recovery were verified with 100M rows and up to 256 clients;
- graceful and process-kill reopen were verified;
- a real transient ATA reset did not cause corruption;
- prolonged I/O starvation did not become an irreversible livelock;
- the final snapshot/restore/digest oracle passed.

Acceptance does not mean:

- an SLA for this HDD;
- a guarantee under permanent device loss, false flush or power loss;
- that the degraded storage is suitable for production;
- that a 24h/72h repeat on this host is required.
