# CA-80.7 — сравнительный NVMe-бенчмарк 100M

[English](CA_80_7_100M_COMPARATIVE_REPORT.md)

Дата: 2026-09-06.

## Решение

CA-80.7 принят. Candidate сохранил checksum и одинаковые cardinality, bounded
стоимость последующих seed chunks и уложился в обязательный коридор `<=1,20x`
относительно предыдущего RadixDB по всем сопоставимым временным метрикам, кроме
`delete.rollback` полного mixed-профиля. Это одно отклонение принято как явное
исключение: отдельный DELETE run дал `2,267 ms`. Правило `20%` не изменено;
исключение не переносится на другие метрики и будущие прогоны.

Главный физический результат новой эпохи: active database стала на `26,3%`
компактнее, fresh final RSS — на `93,8%` ниже, а verify peak RSS — на `68,5%`
ниже. Fresh lifecycle замедлился на `8,8%`, index build — на `15,2%`; оба
результата находятся внутри принятого коридора.

## Идентичность и методика

- candidate source: `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc`;
- Cargo.lock SHA-256:
  `a0dfe4bc1679224fadc97f983574a92e50d326c4f3c3ad44d1c9ce550e1dcfbc`;
- exact release `radixdb-bench` SHA-256:
  `1b2500760e957dbc0bbce94b3d6be6bf19f4e6c7599d2518b46bb4eef4684d5d`;
- build: release, `bench-harness`, production-default `mimalloc`;
- host: AMD Ryzen 9 7950X, Apacer AS2280Q4U 2 TB NVMe, Btrfs,
  Linux `7.1.3-201.fc44.x86_64`;
- dataset: неизменный 100M fixture, checksum
  `100000000:49734600639880`, page-cache level `0`, storage workers `auto`;
- fresh previous-RadixDB evidence: revision
  `aada83a4a46cdf80caa541e4bc0acc7c21a26bf1`;
- canonical previous-RadixDB verify baseline: revision
  `cce945773038e150845760e82327d81c5cdb2eae`,
  [`FINAL_100M_NVME_ACCEPTANCE_REPORT.ru.md`](FINAL_100M_NVME_ACCEPTANCE_REPORT.ru.md);
- frozen old-engine index baseline: `115 158,500 ms`, как зафиксировано до
  candidate-run в CA-80.5;
- frozen PostgreSQL baseline:
  [`icp621-pg18-page-cache-matrix-100m-20260828.ru.md`](icp621-pg18-page-cache-matrix-100m-20260828.ru.md),
  PostgreSQL `18.3`, тот же host и dataset.

PostgreSQL не перезапускался 2026-09-06: используется сохранённый полный
fresh-run от 2026-08-28. Его COPY выполнялся с `synchronous_commit=off` и
отключённым autovacuum, поэтому import приведён как throughput reference, а не
как durability-equivalent сравнение с синхронными RadixDB commits.

Candidate fresh-run выполнен один раз. Verify-existing выполнен трижды одним
binary: run 1 после адресного `DONTNEED` файлов database, runs 2/3 без eviction.
В каждой query series один warm-up исключался, затем бралась median пяти
измерений. Сводная latency ниже — median трёх run-level medians; cold start и
resource elapsed приведены отдельно, поэтому холодное и прогретое состояние не
скрыто общей средней.

## Свежий жизненный цикл

| Фаза | Candidate | Previous RadixDB | Delta | PostgreSQL 18.3 | Candidate / PG |
|---|---:|---:|---:|---:|---:|
| полный participant | `572,329 s` | `525,955 s` | `+8,817%` | `182,558 s` | `3,135x` |
| COPY 100M | `429,087 s` | `393,930 s` | `+8,925%` | `100,858 s` | `4,254x` |
| CREATE INDEX | `132,690 s` | `115,159 s` frozen | `+15,224%` | `81,595 s` | `1,626x` |

Candidate COPY: `233 053 rows/s`. Все `120` chunks имеют фиксированную
cardinality около `833 333`; median всех chunks `3 483,246 ms`, steady window
13..108 — `3 484,360 ms`, последние 12 — `3 477,425 ms`. Late/steady ratio
`0,998x`, max/overall-median `1,683x`: роста стоимости chunk от размера уже
загруженной базы нет.

## Размер, память и запись

| Метрика | Candidate | Previous RadixDB | Delta |
|---|---:|---:|---:|
| fresh peak RSS | `2 176 487 424 B` | `36 376 854 528 B` | `-94,0%` |
| fresh final RSS | `1 241 767 936 B` | `19 942 359 040 B` | `-93,8%` |
| process write bytes | `30 400 126 976 B` | `25 533 755 392 B` | `+19,1%` |
| device write bytes | `28 044 476 416 B` | `26 062 774 272 B` | `+7,6%` |
| logical database | `1 964 220 021 B` | `2 666 165 781 B` | `-26,3%` |
| allocated database | `1 965 780 992 B` | `2 667 888 640 B` | `-26,3%` |
| files / directories | `454 / 148` | `364 / 125` | bounded artifact layout |

PostgreSQL database занимает `16 126 596 799 B`, то есть `8,21x` candidate;
полный cluster вместе с WAL — `32 885 882 880 B`, или `16,74x` candidate.
Это storage footprint, а не измерение физических write bytes.

## Проверка существующей базы и ресурсы

| Run | Начальное состояние cache | Elapsed | Peak RSS | Final RSS | Process read |
|---|---|---:|---:|---:|---:|
| 1 | database files evicted | `9,266 s` | `547 303 424 B` | `311 193 600 B` | `116 617 216 B` |
| 2 | cache-hot | `8,837 s` | `538 542 080 B` | `293 380 096 B` | `0 B` |
| 3 | cache-hot | `8,947 s` | `611 880 960 B` | `283 430 912 B` | `0 B` |
| median | явно смешанное только для сводки | `8,947 s` | `547 303 424 B` | `293 380 096 B` | — |

Предыдущий canonical verify: `14,274 s`, peak RSS `1 738 457 088 B`, active
database `2 666 165 783 B`. Candidate соответственно быстрее на `37,3%`,
использует на `68,5%` меньше peak RSS и хранит на `26,3%` меньше данных. Все
три runs прошли checksum/cardinality/rollback oracle; canonicalized access-path
SHA-256 у candidate одинаков:
`da6d4c0bfc27131ce3c2403791e28f3050c5ca7187e4981f10531cb797bdfccd`.

## Задержки запросов

Отрицательная delta означает ускорение. `Candidate / PG < 1` означает, что
candidate быстрее PostgreSQL. Navigation syntax отсутствует в PostgreSQL;
соответствующие строки используют описанный в PG baseline classic JOIN analog.

| Case | Candidate, ms | Previous RDB, ms | Delta | PG, ms | Candidate / PG |
|---|---:|---:|---:|---:|---:|
| `correctness.seed_checksum` | `11,638` | `12,497` | `-6,9%` | `5823,818` | `0,002x` |
| `select.pk` | `0,490` | `0,481` | `+1,9%` | `0,237` | `2,068x` |
| `select.range` | `0,475` | `0,485` | `-2,0%` | `0,269` | `1,768x` |
| `scan.full` | `100,299` | `93,247` | `+7,6%` | `246,457` | `0,407x` |
| `scan.projected` | `23,189` | `21,055` | `+10,1%` | `84,456` | `0,275x` |
| `aggregate.group_having` | `9,907` | `10,286` | `-3,7%` | `24,948` | `0,397x` |
| `join.parent` | `20,790` | `17,945` | `+15,9%` | `12,574` | `1,653x` |
| `reference.navigation` | `31,972` | `27,313` | `+17,1%` | `9,283` analog | `3,444x` |
| `reference.direct.explicit` | `20,190` | `18,233` | `+10,7%` | `8,918` | `2,264x` |
| `reference.fact_dictionary` | `592,686` | `725,155` | `-18,3%` | `344,862` analog | `1,719x` |
| `reference.fact_first` | `376,190` | `415,987` | `-9,6%` | `358,814` | `1,048x` |
| `reference.target_first` | `354,408` | `378,050` | `-6,3%` | `379,936` | `0,933x` |
| `update.rollback` | `52,568` | `44,532` | `+18,0%` | `13,360` | `3,935x` |
| `delete.rollback` | `3,180` | `1,746` | **`+82,1%`** | `2,111` | `1,506x` |

### Принятое DELETE-исключение

Полный профиль выполняет UPDATE и DELETE последовательно и показывает
устойчиво более дорогой DELETE относительно прежней revision. Чтобы отделить
его от соседней операции, exact candidate был запущен отдельно: после одного
warm-up пять samples дали median `2,267 ms` (`2,216..2,284 ms`). Это `+7,4%`
к PG `2,111 ms`, но `+29,8%` к previous-RadixDB `1,746 ms`.

Отдельный ранний 20k micro-reference (`0,155 ms` RadixDB против сообщённых для
сравнения `2,3 ms` PostgreSQL, около `14,8x` быстрее) не используется для
100M verdict: это другой масштаб и другая cardinality.

## Исходные доказательства

- fresh: `RadixTest/results/ca80-7-final-100m-b648b2d3-20260906`;
- verify: `ca80-7-verify-100m-b648b2d3-r{1,2,3}-20260906`;
- isolated DELETE: `ca80-7-delete-isolated-100m-b648b2d3-20260906`;
- previous fresh: `RadixTest/results/icp7-current-100m-fresh-aada83a`;
- PostgreSQL: `RadixTest/results/icp621-matrix-pg18-100m-92b1dcf`.

SHA-256 `results.json / REPORT.md`:

- fresh: `ad6b654ab342107299ce22cccb12106e6d2b9034419d80df2bc00b8ee5f5f57c` /
  `b8b4b5f59d10c2d916eb11af1381aa652f76b9aac8c7ab952b43dec984cb44b9`;
- verify r1: `928fbdf59752baea11e9465da0e08aa4ba11e482a9c5d22e2e9ab83b78570f6e` /
  `e4df474a132f5c4da0ca280736607f19857aa4c39deabe6fdf5a41848da7a1f4`;
- verify r2: `079d78da310f3c9ac650e6a9d3ad876cf96325ee9411b81937b17a0bf2016613` /
  `34ed4e50e64ed6ab256a24e33e0209c861f71f975729107c13aa3043088ea929`;
- verify r3: `fb04b56e5339640788ce0db9cad5fd34f8c4ca17cda1e3042829244dc7d1a8c6` /
  `6a3f4d37bb5ae03a8094af5d1ce3cf1e4853e10aa692546cf5a9c4a1a78dc422`;
- isolated DELETE: `0275449addd99000df324615ec930af1fc15044de191194f77d59b82b89a51c4` /
  `6b3e8df20667ed19ebc36da78e483966a5f56e35ccd77c53c72725f1b1635ca9`;
Raw artifacts находятся вне Git; в Git сохраняется этот компактный
воспроизводимый паспорт.
