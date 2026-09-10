# RadixDB 1.1: 100-million-row release validation

[Русский](RADIXDB_1_1_100M_VALIDATION.ru.md)

Date: 2026-09-08  
Status: **PASS**

## Identity and method

The accepted query series used performance source `12ef5963`, the unchanged
canonical 100-million-row NVMe/Btrfs database, `page-cache-level=0`, one
excluded warm-up and five measured repeats in each of three predeclared
restart-hot runs. Runs with a monotonic operating-system cache warm-up trend
were discarded before comparison.

The release tag is `v1.1.0` at commit
`804027a4ee8426b6f7bd083c6e26a1895603dc38`. The performance source is kept
separate from later correctness hardening and is not relabelled as a newer
benchmark binary.

## Result

| Case | 1.1 candidate median | Accepted baseline | Delta |
| --- | ---: | ---: | ---: |
| Aggregate | 10.150 ms | 9.907 ms | +2.45% |
| Checksum | 12.007 ms | 11.638 ms | +3.17% |
| Fact dictionary | 572.626 ms | 592.686 ms | -3.38% |
| Full scan | 105.224 ms | 100.299 ms | +4.91% |
| UPDATE rollback | 42.994 ms | 52.568 ms | -18.21% |
| DELETE rollback | 2.285 ms | 3.180 ms | -28.13% |

All three accepted runs preserved 100,000,000 rows, checksum
`100000000:49734600639880` and access-path digest
`499488e7eeca18a91a2ebb763473308e15d4e929a7c0770d933c9fd9bba084d4`.
Their peak RSS values were 563,662,848, 566,497,280 and 551,399,424 bytes.
The worst listed regression was 4.91%, below the predeclared 1.20-times
corridor.

Release binary SHA-256:
`d28acefb30cde0224e5298e67237933e9e117ec7a6e2bd1910a514434a1ae1d2`.
Cargo.lock SHA-256:
`a2b5a5a1c19da45ad29cdb05ce7d7fbaff40a766ef3727291c7edc89475171cb`.

The raw result directories are not distributed in the public repository. This
compact record preserves the accepted identities, method and release values.
