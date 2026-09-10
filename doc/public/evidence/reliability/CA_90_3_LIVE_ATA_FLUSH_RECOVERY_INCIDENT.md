# CA-90.3 live ATA flush recovery incident

[Русский](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md)

Status: recorded real-hardware incident with autonomous recovery; the complete
six-hour soak finished and was accepted as PASS.

Event time: 2026-09-07 17:06–17:34 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Tested binary: `dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

Complete selected excerpts without paraphrasing are preserved in
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log).

## 1. What happened

During the live CA-90.3 run, the host received 15 minutes of direct competing
random writes to the same ext4 filesystem. The load was intentional, but the
storage-path failure was not emulated with a failpoint, device mapper or
software error: the kernel recorded a real ATA bus error on `FLUSH CACHE EXT`,
a connection-state change and a hard reset of the physical SATA link.

After the reset, the kernel brought the already degraded link back at
`1.5 Gbit/s + UDMA/33`, retried `FLUSH` and completed error handling. RadixDB
did not exit, the filesystem did not become read-only, the watchdog did not
expire, the invariant oracle found no mismatch, and the workload resumed
progress on its own before advancing to the next client stage.

This was not a synthetic `EIO` substitution. It was an observed transient loss
of the drive or interface during a durability operation on a running database.

## 2. Exact timeline

| Time +07 | Event |
|---|---|
| 17:06:05 | Bounded side-I/O pressure starts; CA-90.3 is at `clients-128`, `79 491` commits, `0` invariant failures |
| 17:15:09 | After 9 minutes of competing writes, semantic progress is silent for `547 470 ms`; watchdog remains healthy |
| 17:19:10 | Still under load, the engine resumes progress without intervention: `80 434` commits, `+943` from the window start |
| 17:21:05 | The direct writer stops on its 900-second timer after creating `29 657 923 584` bytes of random data |
| 17:21:10 | Post-writer snapshot: run alive, watchdog healthy, invariants `0` |
| 17:26:10 | Planned five-minute recovery observation completes; side-load unit succeeds |
| 17:26:31.173 | `FLUSH CACHE EXT` receives an ATA bus error, link freezes, hard reset starts |
| 17:26:31.905 | SATA link comes back at `1.5 Gbps` |
| 17:26:31.913 | Device is configured again for `UDMA/33` |
| 17:26:31.914 | Kernel retries `FLUSH`; ATA error handling completes |
| 17:34:52 | Run is at `clients-256`: `82 024` commits, TPS `2.707`, watchdog healthy, `0` invariant failures, all three soak units active |

From the start of pressure to the 17:34:52 snapshot, the commit count increased
by `2 533`. Progress resumed both during the direct competing writes and after
the physical reset. For the observed window, this rules out the explanation
that the process remained alive while the workload was irreversibly stuck.

## 3. Kernel evidence

Key kernel-journal excerpt:

```text
2026-09-07T17:26:31.173789+07:00 debian kernel: ata3.00: exception Emask 0x10 SAct 0x0 SErr 0x4050000 action 0xe frozen
2026-09-07T17:26:31.174033+07:00 debian kernel: ata3.00: irq_stat 0x00000040, connection status changed
2026-09-07T17:26:31.174111+07:00 debian kernel: ata3: SError: { PHYRdyChg CommWake DevExch }
2026-09-07T17:26:31.174187+07:00 debian kernel: ata3.00: failed command: FLUSH CACHE EXT
2026-09-07T17:26:31.174281+07:00 debian kernel: ata3.00: cmd ea/00:00:00:00:00/00:00:00:00:00/a0 tag 29
2026-09-07T17:26:31.174350+07:00 debian kernel: ata3.00: status: { DRDY }
2026-09-07T17:26:31.174414+07:00 debian kernel: ata3: hard resetting link
2026-09-07T17:26:31.905837+07:00 debian kernel: ata3: SATA link up 1.5 Gbps (SStatus 113 SControl 310)
2026-09-07T17:26:31.913837+07:00 debian kernel: ata3.00: configured for UDMA/33
2026-09-07T17:26:31.914022+07:00 debian kernel: ata3.00: retrying FLUSH 0xea Emask 0x10
2026-09-07T17:26:31.914103+07:00 debian kernel: ata3: EH complete
```

The kernel journal contained no `Buffer I/O error`, ext4 error, critical medium
error or read-only remount in the same window. After the event, the root
filesystem on `/dev/sda1` remained mounted as
`ext4 rw,relatime,errors=remount-ro`.

## 4. RadixDB response

During starvation, the engine did not report false maintenance success. It left
unfinished work for bounded retry:

```text
2026-09-07T17:20:56.944842+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:20:59.129455+07:00 Warning: immutable-member retirement requires retry: artifact cleanup wall-time nanoseconds is 2061232602; configured limit is 2000000000
2026-09-07T17:25:23.631488+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:28:21.750294+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:28:23.852990+07:00 Warning: immutable-member retirement requires retry: artifact cleanup wall-time nanoseconds is 2000795172; configured limit is 2000000000
```

Snapshot after the physical reset:

| Metric | Value |
|---|---:|
| State / phase | `running / clients-256` |
| Clients | `256/256` |
| Current TPS | `2.707` |
| Transactions planned | `103 889` |
| Transactions committed | `82 024` |
| Transactions rolled back | `11 910` |
| Conflicts | `3 978` |
| Operations | `2 091 985` |
| Invariant passes / failures | `2 040 / 0` |
| Reopens | `2` |
| Watchdog | `healthy`, silence `131 271 / 1 260 000 ms` |
| Terminal failure | none |

All three units, the agent, database server and observer, remained active.

## 5. Observer evidence

The observer detected the failure itself; it was not supplied through manual
interpretation:

```json
{
  "id": "incident-000104",
  "alert_ids": ["disk-degradation-13014"],
  "candidate_cause": "disk_degradation",
  "confidence": "confirmed",
  "artifact_directory": "incidents/000104-disk-degradation",
  "closed_unix_millis": null
}
```

The alert has `severity=fatal` and remains active as a confirmed hardware-path
defect. This severity describes a diagnostic storage event, not a terminal
database-process failure: at the same time, `observer-status.json` reported
`terminal=false`, `telemetry_gaps=0` and no failure.

The complete incident bundle is stored outside the public Git repository.
Selected timestamped excerpts are included in the published evidence log.

At the first snapshot, the incident was still open and correctly listed the
missing post-trigger files `engine-after.json` and `threads-after.json`. This
does not invalidate the kernel evidence, but prevents the incident bundle from
being called complete before terminal evidence collection.

## 6. SMART after the event

Immediately after the incident, SMART still reported:

| Counter | RAW |
|---|---:|
| `Reallocated_Sector_Ct` | `0` |
| `Current_Pending_Sector` | `0` |
| `Offline_Uncorrectable` | `0` |
| `UDMA_CRC_Error_Count` | `3` |
| ATA Error Count | `3` |
| Power-on hours | `3 364` |
| Temperature | `43°C` |

The new kernel ATA reset did not increment the SMART error log. SMART is
therefore useful but insufficient as an oracle: the live transport failure was
visible to the kernel and observer even though the drive did not record a new
SMART error.

## 7. Proven boundary

The incident proves that:

- RadixDB continued operating through a real transient ATA transport failure
  at the `FLUSH CACHE EXT` boundary;
- the kernel restored the link and completed its retry without a permanent
  application-visible `EIO`;
- bounded retry did not turn into the previously identified infinite
  generation loop;
- after recovery, commits continued, the client stage increased, invariant
  failures remained `0`, and the watchdog was healthy;
- the observer recognized the degradation and retained incident evidence.

The incident by itself does not prove:

- survival after permanent device loss or unrecoverable `EIO`;
- correctness of a device that falsely acknowledges a flush;
- power-loss durability after physically removing power;
- the successful terminal verdict of the full CA-90.3 before final
  quiescence/reopen;
- any normative HDD or NVMe performance.

This was a rare production-realistic event: the competing I/O load was
intentional, but the particular ATA bus error, hard reset and retry occurred on
real degrading hardware and could not be controlled by the test oracle.

## 8. Subsequent stalls and progress recovery

After recovery from the ATA reset, the engine advanced from `clients-128` to
`clients-256` and continued to `85 770` commits. The last recorded semantic
progress was at 17:42:29.628 +07. The accumulated maintenance tail on the
degraded disk then temporarily exceeded the watchdog corridor:

| Snapshot | Value |
|---|---:|
| Observation time | `18:07:39 +07` |
| Phase | `clients-256` |
| Commits | `85 770` |
| Operations | `2 150 604` |
| Silence | `1 509 571 ms`, approximately `25:09` |
| Watchdog limit | `1 260 000 ms`, `21:00` |
| Watchdog state | `stalled` |
| Invariant failures | `0` |
| Process / units | alive / active |
| Terminal failure | none |

This snapshot did not become a terminal liveness failure. The engine continued
working against the I/O backlog and returned from `stalled` to `healthy` twice:

| Time +07 | Recovery result |
|---|---|
| 18:17:15 | `91 021` commits, TPS `5.042`, watchdog `healthy` again |
| 18:29:56 | Second `stalled`: `91 021` commits, silence `1 286 635 ms` |
| 18:30:57 | `95 361` commits, another `+4 340`; TPS `4.888`, silence `34 624 ms`, watchdog `healthy` |

The honest proven boundary of the observation is therefore:

- `clients-128` survived 15 minutes of direct I/O starvation, a real ATA bus
  error on flush, a hard link reset and subsequent progress recovery;
- `clients-256` was reached, executed transactions and resumed progress twice
  after exceeding the 21-minute watchdog corridor;
- the `stalled` states were real and remain in the evidence, but did not become
  a terminal failure or irreversible loss of progress;
- no corruption was found: the latest invariants passed, the filesystem stayed
  read-write, and no new kernel I/O errors appeared after the reset;
- the most likely limiting factor was the degraded HDD/SATA path's inability
  to retire accumulated physical maintenance work quickly enough. Before the
  run completed its terminal evidence, this was a justified attribution rather
  than a proven single root cause.

The intentional side load and the hardware reset make this a strengthened run,
not an ordinary clean six-hour acceptance test. The terminal checkpoint,
source/restored invariants, snapshot/restore and logical digest subsequently
passed. The run is accepted as PASS; the intermediate `stalled` state is not
treated as a failure of `clients-256` because later progress and the complete
terminal oracle were directly observed.

## 9. Terminal outcome

The run completed with `state=passed`:

- `98 668` commits and `2 351 035` operations;
- `2 100/0` invariant passes/failures;
- `2` successful checkpoints, `1` snapshot and `1` restore;
- source and restored logical digests matched;
- the agent exited with `Result=success`;
- the observer reached `terminal=true` without telemetry gaps.

Final acceptance report:
[`CA_90_3_6H_ACCEPTANCE_REPORT.md`](CA_90_3_6H_ACCEPTANCE_REPORT.md).

## 10. Workload reproducibility

The bounded driver is stored as
[`side-io-pressure.sh`](../../../../crates/radixdb-soak/deploy/atom-hdd/side-io-pressure.sh).
It schedules 900 seconds of direct random writes and 300 seconds of recovery
observation, and checks the watchdog budget, free space, run state and invariant
failures.

The physical ATA failure itself is not considered reproducible: the script
reproduces only the I/O-starvation conditions and does not promise another
hardware failure.
