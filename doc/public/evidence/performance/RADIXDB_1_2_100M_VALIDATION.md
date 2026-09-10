# RadixDB 1.2: validation on a 100-million-row database

[Русский](RADIXDB_1_2_100M_VALIDATION.ru.md)

Date: 2026-09-09
Status: **PASS**

## Purpose

This validation confirms that the existing database with 100,000,000 rows
remains accessible after the RadixDB 1.2 server, security and reliability layer
changes. It validates the current version; it does not repeat the cross-engine
benchmark. The PostgreSQL and earlier RadixDB values in the comparison table
were not remeasured.

## Identity and environment

| Parameter | Value |
| --- | --- |
| Source SHA | `23bf35df011aae6816d77578be96074b02bc363c` |
| Dataset | canonical 100M database |
| Device | `nvme1n1` |
| Data check | `100000000:49734600639880` |

## Result

| Metric | Value |
| --- | ---: |
| Measured run | 11,870.421 ms |
| Cold select-database | 523.413 ms |
| Peak RSS | 565.97 MiB |
| Final RSS | 343.04 MiB |
| Allocated database size | 1.83 GiB |
| Storage/resource errors | 0 |

The validation preserved the exact cardinality and checksum. Every value above
applies to the stated source SHA and this specific NVMe run. The values are not
an SLA, a universal capacity estimate or a new PostgreSQL comparison.

The complete raw bundle is stored outside the public Git repository. This
public record preserves its identity, conditions and accepted final metrics.
