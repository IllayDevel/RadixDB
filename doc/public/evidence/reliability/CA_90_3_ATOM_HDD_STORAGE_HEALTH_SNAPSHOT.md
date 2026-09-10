# CA-90.3 Atom/HDD storage health snapshot

[Русский](CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.ru.md)

Status: live hardware evidence; the associated six-hour soak completed and was
accepted as PASS.

Update after the initial snapshot: at 17:26:31 +07 the same live run
experienced a real ATA bus error on `FLUSH CACHE EXT`, a hard reset and a kernel
retry. The complete record and logs are in
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md).

Snapshot time: 2026-09-07 15:21–15:30 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Tested binary: `dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

## 1. Purpose

This document records the actual state of the storage path on which CA-90.3
ran. It separates engine properties from host limitations and degradation. The
snapshot was taken without stopping or reconfiguring the soak.

Performance on this host is not an NVMe baseline or a product SLA. Because the
run completed successfully, its correctness and recovery evidence may be
interpreted as a test on a substantially degraded drive and interface.

## 2. Drive and filesystem

| Parameter | Value |
|---|---:|
| Model | Toshiba MQ01ABD050, 2.5-inch HDD |
| Manufacturing year | 2013, from host label/records; SMART does not report the year |
| Rotation speed | 5400 rpm |
| Logical / physical sector | 512 / 4096 bytes |
| ATA/SATA | ATA8-ACS, SATA 2.6 |
| Advertised SATA link speed | 3.0 Gbit/s |
| Link speed at snapshot | 1.5 Gbit/s |
| Negotiated transfer mode after degradation | UDMA/33 |
| Available capacity | 320,072,933,888 bytes, approximately 298.1 GiB |
| Native capacity | 976,773,168 sectors, approximately 500 GB |
| HPA | enabled: `625142449/976773168` sectors |
| `/storage` location | `/dev/sda1`, shared root filesystem |
| Filesystem | ext4, `rw,relatime,errors=remount-ro` |

The model nominally belongs to the 500 GB class, but the BIOS/host HPA limits
visible capacity to approximately 320 GB. This is not a partitioning result or
a RadixDB parameter.

## 3. SMART lifetime counters

| Metric | RAW | Assessment |
|---|---:|---|
| Overall health | `PASSED` | vendor threshold not crossed |
| Power-on hours | `3 362` | 140 days 2 hours |
| Power cycles | `642` | informational |
| Start/stop count | `647` | informational |
| Load cycle count | `51 906` | notable mechanical history; threshold not crossed |
| Loaded hours | `913` | time with heads loaded |
| Power-off retract count | `35` | emergency or abnormal head parking |
| G-sense events | `2` | recorded shocks/accelerations |
| Reallocated sectors | `0` | no surface-remap evidence |
| Reallocation events | `0` | no surface-remap evidence |
| Current pending sectors | `0` | none awaiting remap |
| Offline uncorrectable | `0` | no uncorrectable offline sectors recorded |
| Reported uncorrectable errors | `0` | no media read/write failure recorded |
| Spin retry / load retry | `0 / 0` | no repeated mechanical start |
| UDMA CRC errors | `3` | transport/interface fault history |
| SMART device error log | `3` | three `ICRC, ABRT` at power-on hour 3,135 |
| Hardware resets | `6 746` | high lifetime reset count |
| PhyRdy → PhyNRdy transitions | `978 175` | abnormally high link-transition history |
| Lifetime logical writes | `703 903 210` sectors | approximately 335.65 GiB |
| Lifetime logical reads | `529 079 240` sectors | approximately 252.28 GiB |
| Temperature | `43°C` | lifetime min/max `14/47°C`, over-temperature `0` |

The last recorded short self-test completed without error at power-on hour
3,156. A full extended self-test was not started for this snapshot: its stated
duration is 116 minutes, and additional I/O would have distorted the active
soak.

## 4. Degradation during the current boot

The current operating-system boot began at 2026-09-04 08:22:25 +07. Its kernel
journal contained:

| Kernel evidence | Count/result |
|---|---:|
| `ata3: SATA link up` | `21` |
| link up at 3.0 Gbit/s | `10` |
| link up at 1.5 Gbit/s | `11` |
| explicit speed-limit events | `3` |
| media/filesystem I/O errors | `0` |

Before the initial snapshot on September 7, the kernel progressively degraded
the transport:

```text
09:58:59  SATA link limited: 3.0 -> 1.5 Gbit/s
10:06:27  transfer mode limited: UDMA/100 -> UDMA/66
10:11:46  transfer mode limited: UDMA/66 -> UDMA/33
```

The link came back after every limitation. At snapshot time ext4 remained
read-write, and the kernel journal contained no `Buffer I/O`, uncorrectable
media or filesystem errors.

After the snapshot, at 17:26:31, the kernel journal confirmed not only parameter
degradation but a live ATA bus error with a hard link reset. Therefore the
`media/filesystem I/O errors = 0` row above applies strictly to the original
15:21–15:30 interval, not the whole later run. No permanent media error or ext4
error was observed after the new transport incident either.

## 5. Conclusion

`SMART overall-health PASSED` does not mean that this storage path was healthy.
SMART shows no surface degradation, but lifetime counters and the current
kernel journal prove SATA-path instability with automatic reductions in speed
and transfer mode. From the application's perspective, whether the primary
cause lies in the HDD, connector, cable, power or SATA controller is
irrelevant: the host's complete durable I/O path is degraded.

At snapshot time, the findings were classified as follows:

- media integrity: no confirmed bad, pending or uncorrectable sectors;
- transport stability: confirmed severe degradation;
- filesystem integrity: no error or read-only remount;
- soak correctness: assessed separately by the CA-90.3 terminal oracle;
- performance: not transferable to other platforms and not a normative value.

## 6. Checks through terminal result

The same counters were collected again after the soak. Any of the following
would be a separate storage failure:

- an increase in `Reallocated_Sector_Ct`, `Current_Pending_Sector` or
  `Offline_Uncorrectable`;
- an increase in `UDMA_CRC_Error_Count` or the SMART error log;
- a new link reset or speed downgrade;
- a kernel media I/O error;
- an ext4 error or read-only remount;
- a mismatch in the final crash/reopen oracle.

Snapshot sources were read-only `smartctl -x /dev/sda`, `hdparm -N /dev/sda`,
`lsblk`, `findmnt` and the current boot's kernel journal. The serial number and
WWN are intentionally omitted from the public document.

## 7. Subsequent live evidence

At 17:26:31 the "new link reset" criterion occurred: `FLUSH CACHE EXT` ended
with a bus error, after which the kernel performed a hard reset, brought the
link back and retried the flush. The observer opened confirmed
`incident-000104` in the `disk_degradation` class.

After recovery, the run continued committing and advanced from `clients-128`
to `clients-256`; the watchdog remained healthy, invariant failures remained
`0`, and ext4 remained read-write. SMART media, CRC and error counters did not
change, separately demonstrating why kernel telemetry is needed alongside
SMART.

Later, at `clients-256`, the accumulated maintenance tail exceeded the
21-minute watchdog corridor twice. Both times the processes stayed alive and
the engine independently returned from `stalled` to `healthy`, continuing to
first `91 021` and then `95 361` commits. Invariant failures remained `0`, and
no new kernel storage errors appeared. The intermediate stalls therefore did
not fail the 256-client stage; the final verdict depended on the terminal
checkpoint and invariants.

The exact timeline, raw excerpts and proven boundary are recorded in
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md).

The terminal checkpoint, snapshot/restore and logical digest subsequently
passed with `2 100/0` invariant passes/failures. Final verdict:
[`CA_90_3_6H_ACCEPTANCE_REPORT.md`](CA_90_3_6H_ACCEPTANCE_REPORT.md).
