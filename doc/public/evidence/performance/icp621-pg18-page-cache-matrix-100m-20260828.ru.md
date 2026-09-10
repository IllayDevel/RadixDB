# ICP-6.21 — матрица page cache PostgreSQL 18 на 100M

[English](icp621-pg18-page-cache-matrix-100m-20260828.md)

Дата: 2026-08-28.

## Решение

Сравнительный 100M gate пройден. PostgreSQL 18.3 заново создал schema,
загрузил 100M строк и построил индексы; текущий RadixDB затем четыре раза
открыл один current-format 100M fixture с `page_cache_level = 0/1/5/10`.
Перед каждым RadixDB run harness адресно вызывал Linux `DONTNEED` только для
файлов benchmark database, после чего явно ждал завершения warmup.

Во всех пяти участниках совпали checksum `100000000:49734600639880`,
cardinality запросов и rollback results. Storage footprint RadixDB не менялся
между уровнями cache. Production default остаётся `0`.

## Идентичность и методика

- Tree HEAD во время matrix: `92b1dcf`; production engine/binary source:
  `e27d98e21f04b3a46136fc02c36f8f508a9f9c34`. Последующие два коммита меняли
  только acceptance test и документацию.
- Release binary SHA-256:
  `6d73eec7f1423d3e9c45023bbea9ee5f290c48f19ba3773f5f72c4c5a8778377`.
- Cargo.lock SHA-256:
  `84a717843ff6e896245d0f46f63c361ba0c3816c89d38b1aef41999f8fac7737`.
- PostgreSQL `18.3`, dedicated ownership-verified cluster, профиль
  `nvme-local`: `shared_buffers=4GB`, `effective_cache_size=24GB`,
  `maintenance_work_mem=2GB`, `work_mem=256MB`, parallel gather `4`, JIT off.
- PostgreSQL bulk profile использует `synchronous_commit=off` и autovacuum off;
  это явный throughput contract. Его import нельзя выдавать за durability
  comparison с синхронным commit.
- PostgreSQL проходил fresh lifecycle. RadixDB использовал один неизменный
  existing-data fixture: это намеренно изолирует влияние page-cache policy и
  не смешивает его с четырьмя повторными imports.
- UPDATE/DELETE RadixDB — median `31` измерения после `5` warm-up. Полный PG
  lifecycle содержит одно измерение DML; оно приведено как внешний reference.
- `% PG` равен `RadixDB time / PostgreSQL time`; меньше `100%` означает, что
  RadixDB быстрее. `Δ10/0` сравнивает полный warmup с cache level `0`.

## Полная сравнительная таблица

| Case | PG, ms | RDB L0, ms | L0 / PG | RDB L1, ms | L1 / PG | RDB L5, ms | L5 / PG | RDB L10, ms | L10 / PG | Δ10/0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `correctness.seed_checksum` | 5823.818 | 13.419 | 0.2% | 14.996 | 0.3% | 13.994 | 0.2% | 12.849 | 0.2% | -4.2% |
| `select.pk` | 0.237 | 0.480 | 202.3% | 0.482 | 203.0% | 0.486 | 205.0% | 0.489 | 206.3% | +2.0% |
| `select.range` | 0.269 | 0.476 | 177.3% | 0.498 | 185.3% | 0.475 | 176.7% | 0.470 | 175.0% | -1.3% |
| `scan.full` | 246.457 | 97.175 | 39.4% | 93.499 | 37.9% | 97.501 | 39.6% | 88.522 | 35.9% | **-8.9%** |
| `scan.projected` | 84.456 | 20.795 | 24.6% | 20.656 | 24.5% | 21.272 | 25.2% | 20.661 | 24.5% | -0.6% |
| `aggregate.group_having` | 24.948 | 10.379 | 41.6% | 10.411 | 41.7% | 10.655 | 42.7% | 10.525 | 42.2% | +1.4% |
| `join.parent` | 12.574 | 20.001 | 159.1% | 19.489 | 155.0% | 20.206 | 160.7% | 19.978 | 158.9% | -0.1% |
| `reference.navigation` | 9.283 | 28.123 | 303.0% | 28.705 | 309.2% | 30.360 | 327.1% | 28.347 | 305.4% | +0.8% |
| `reference.direct.explicit` | 8.918 | 187.332 | 2100.6% | 181.239 | 2032.3% | 183.705 | 2059.9% | 184.753 | 2071.7% | -1.4% |
| `reference.fact_dictionary` | 344.862 | 544.913 | 158.0% | 531.400 | 154.1% | 557.919 | 161.8% | 539.360 | 156.4% | -1.0% |
| `reference.fact_first` | 358.814 | 732.736 | 204.2% | 730.196 | 203.5% | 752.727 | 209.8% | 725.919 | 202.3% | -0.9% |
| `reference.target_first` | 379.936 | 786.354 | 207.0% | 801.102 | 210.9% | 801.850 | 211.0% | 785.340 | 206.7% | -0.1% |
| `update.rollback` | 13.360 | 39.962 | 299.1% | 36.331 | 271.9% | 36.219 | 271.1% | 35.665 | 267.0% | **-10.8%** |
| `delete.rollback` | 2.111 | 1.608 | 76.2% | 1.639 | 77.6% | 1.599 | 75.8% | 1.626 | 77.1% | +1.1% |

PostgreSQL не поддерживает RadixDB navigation syntax. В строках
`reference.navigation` и `reference.fact_dictionary` PG выполняет
соответствующий classic JOIN shape; одинаковые PG rows/checksum используются
как semantic reference. Внутри RadixDB navigation level `10` быстрее
собственного explicit LEFT JOIN в `6.52x` (`28.347` против `184.753 ms`).

## Прогрев и ресурсы RadixDB

| Level | Target/read/resident | Warmup wall | Read rate | Peak RSS | Peak FD | Storage allocated |
|---:|---:|---:|---:|---:|---:|---:|
| `0` | `0 B` | `0.129 ms` | `0 B/s` | `2204.1 MiB` | `134` | `2,667,393,024 B` |
| `1` | `266,605,528 B` | `104.258 ms` | `2.34 GiB/s` | `2206.6 MiB` | `134` | `2,667,393,024 B` |
| `5` | `1,333,027,640 B` | `502.523 ms` | `2.46 GiB/s` | `2185.3 MiB` | `135` | `2,667,393,024 B` |
| `10` | `2,666,055,280 B` | `1022.348 ms` | `2.42 GiB/s` | `2186.1 MiB` | `135` | `2,667,393,024 B` |

Warmup target, bytes read и resident estimate совпали на уровнях `1/5/10`.
Полный прогрев не увеличил process RSS: данные принадлежат page cache ОС, а не
heap движка. Level `10` устойчиво улучшил full scan и UPDATE median, но не
ускорил CPU/planner-bound point, JOIN и navigation cases; небольшие изменения
там находятся в пределах run-to-run шума.

## Жизненный цикл и размер

PostgreSQL fresh lifecycle:

| Phase | Time |
|---|---:|
| schema create | `105.678 ms` |
| COPY 100M | `100.858 s` |
| CREATE INDEX (358) | `81.595 s` |
| create + COPY + indexes | `182.558 s` |

Footprint после загрузки:

| Engine/path | Bytes | GiB | PG / RadixDB |
|---|---:|---:|---:|
| PostgreSQL database (`pg_database_size`) | `16,126,596,799` | `15.02` | `6.05x` |
| PostgreSQL entire data dir, включая WAL | `32,885,882,880` | `30.63` | `12.33x` |
| RadixDB server data | `2,667,393,024` | `2.48` | `1.00x` |

У PostgreSQL после fresh load отдельно находилось около `15.58 GiB` в
`pg_wal`; поэтому database-only и complete-cluster ratios приведены раздельно.

## Исходные артефакты

- PG: `RadixTest/results/icp621-matrix-pg18-100m-92b1dcf`;
- RadixDB full matrix:
  `icp621-matrix-radix-l{0,1,5,10}-100m-92b1dcf`;
- RadixDB isolated DML:
  `icp621-matrix-radix-l{0,1,5,10}-{update,delete}-rollback-repeat-100m-92b1dcf`.

Локальный PostgreSQL был остановлен после сбора артефактов. Production
RadixDB-процесс `/opt/radixdb` не затрагивался.
