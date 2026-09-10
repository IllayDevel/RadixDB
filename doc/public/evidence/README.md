# RadixDB public evidence

[Русский](README.ru.md)

This directory contains the accepted evidence behind performance, storage
footprint, memory-use and reliability statements in the RadixDB manual. It is
part of the public documentation source and is published with the manual.

The collection contains only accepted final reports. Every report is available
in English and Russian; the base `.md` name is English and `.ru.md` is Russian.
Measured values and identities are equivalent across each pair. Large raw
benchmark bundles remain outside Git; where available, the reports retain
their source, binary, lockfile and result SHA-256 identities. The `.log` file
is untranslated primary machine output shared by both languages.

## Performance

- [`CA_80_5C_MICRO_DEVICE_20K_REPORT.md`](performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.md): accepted 20,000-row memory and device-footprint profile.
- [`CA_80_7_100M_COMPARATIVE_REPORT.md`](performance/CA_80_7_100M_COMPARATIVE_REPORT.md): accepted 100-million-row RadixDB/PostgreSQL comparison.
- [`FINAL_100M_NVME_ACCEPTANCE_REPORT.md`](performance/FINAL_100M_NVME_ACCEPTANCE_REPORT.md): previous RadixDB 100M baseline used by the comparison.
- [`icp621-pg18-page-cache-matrix-100m-20260828.md`](performance/icp621-pg18-page-cache-matrix-100m-20260828.md): PostgreSQL 18.3 baseline used by the comparison.
- [`RADIXDB_1_1_100M_VALIDATION.md`](performance/RADIXDB_1_1_100M_VALIDATION.md): accepted RadixDB 1.1 release query and memory validation.
- [`RADIXDB_1_2_100M_VALIDATION.md`](performance/RADIXDB_1_2_100M_VALIDATION.md): current 1.2 correctness and resource verification on the canonical 100M database.

## Reliability

- [`CA_90_3_6H_ACCEPTANCE_REPORT.md`](reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md): accepted six-hour constrained-hardware soak report.
- [`CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md`](reliability/CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md): storage-health context for the run.
- [`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md`](reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md): the observed ATA reset and recovery boundary.
- [`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log): selected timestamped kernel, process and final-state evidence.

Treat every result as evidence for its exact binary, dataset and machine. See
the manual's benchmark appendix for the comparable summary and caveats.
