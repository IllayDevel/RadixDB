---
title: Evidence archive
description: Accepted benchmark, resource-use and reliability evidence distributed with the RadixDB 1.2 manual.
---

This archive contains the compact source reports behind the measured claims in
the manual. Every report has an English `.md` version and a Russian `.ru.md`
version. Both identify the tested source, dataset, machine and method and are
evidence for those exact conditions, not general service-level objectives.

Large raw benchmark bundles are not stored in Git. Where such bundles exist,
the compact report records the available checksums and explains which raw data
was retained externally. The distributed compact record does not expose local
filesystem paths.

## Performance and resource use

| Evidence | Scope |
| --- | --- |
| [20k memory and footprint report](../../../evidence/performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.md) | Small-dataset lower boundary, allocator comparison and on-disk footprint |
| [100M comparative report](../../../evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md) | RadixDB, previous RadixDB and PostgreSQL 18.3 on one dataset and host |
| [Previous RadixDB 100M baseline](../../../evidence/performance/FINAL_100M_NVME_ACCEPTANCE_REPORT.md) | Three accepted NVMe runs used by the comparison |
| [PostgreSQL 18.3 baseline](../../../evidence/performance/icp621-pg18-page-cache-matrix-100m-20260828.md) | Preserved PostgreSQL participant and cache-state matrix |
| [RadixDB 1.1 release validation](../../../evidence/performance/RADIXDB_1_1_100M_VALIDATION.md) | Accepted query medians, checksums, access paths and peak RSS |
| [RadixDB 1.2 100M validation](../../../evidence/performance/RADIXDB_1_2_100M_VALIDATION.md) | Current-source checksum, elapsed time, RSS and allocated size |

## Reliability

| Evidence | Scope |
| --- | --- |
| [Six-hour acceptance report](../../../evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md) | Correctness, resource use, reopen and recovery on constrained hardware |
| [HDD health snapshot](../../../evidence/reliability/CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md) | SMART and host context for the soak run |
| [ATA reset incident report](../../../evidence/reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md) | Timeline, database behavior and the exact boundary of the recovery claim |
| [Selected incident log](../../../evidence/reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log) | Untranslated primary machine output with timestamped kernel, service and terminal-state excerpts |

Use the [benchmarks](../benchmarks/) chapter for a normalized comparison and
the [limits](../limits/) chapter for supported and measured boundaries.
