# Предыдущий эталонный NVMe-прогон RadixDB на 100M

[English](FINAL_100M_NVME_ACCEPTANCE_REPORT.md)

Дата проверки: 2026-09-04.

## Кандидат и среда

- source/build revision: `cce945773038e150845760e82327d81c5cdb2eae`;
- version `0.5.1`, protocol `13`, release profile;
- Cargo.lock SHA-256:
  `8e93a106d8c2665343b0602d993c8e8fa0147dbb0109d6befc5e1a47b7b1da71`;
- binary SHA-256: server
  `3db6c6d0af92a896f477cf2190423f8caa8f6abc73f018cf0d6a2ca11749bd2b`,
  bench
  `729174e885fab5bd7bc01e4eb6b0898c0524067c12d0081c0942bd6e137984c0`;
- AMD Ryzen 9 7950X, Apacer AS2280Q4U 2TB NVMe, Btrfs;
- Linux `7.1.3-201.fc44.x86_64`, Rust/Cargo `1.97.0`;
- page-cache level `0`, storage workers `auto`, release build;
- 100M fixture и workload неизменны относительно двух предыдущих baselines.

## Команда

```bash
target/release/radixdb-bench \
  --run --verify-existing --scale 100m --participant server \
  --expected-checksum 100000000:49734600639880 \
  --warmup 1 --repeats 5 --allow-large \
  --root /path/to/benchmark-root \
  --run-id radixdb-final-100m-rN-20260904
```

Выполнены три последовательных запуска одним бинарником. Опубликованное значение в
сравнительной таблице — медиана трёх run-level результатов; каждый отдельный
query result уже является медианой пяти measured повторов после одного
warm-up.

## Корректность и ресурсы

Все три запуска подтвердили checksum
`100000000:49734600639880`, ожидаемые cardinality и отсутствие ошибок.

| Метрика | Run 1 | Run 2 | Run 3 | Медиана | Предыдущий baseline | Delta |
|---|---:|---:|---:|---:|---:|---:|
| полный verify elapsed | 14,337 с | 13,501 с | 14,274 с | 14,274 с | 14,800 с | -3,6% |
| peak RSS | 1,60 GiB | 1,64 GiB | 1,62 GiB | 1,62 GiB | 1,62 GiB | 0% |
| peak threads | 39 | 39 | 39 | 39 | 39 | 0 |
| peak open FDs | 135 | 134 | 134 | 134 | 135 | -1 |
| process read | 852 MiB | 0 B | 0 B | 0 B | 0 B | cache-hot parity |
| device read | 852 MiB | 16 KiB | 32 KiB | 32 KiB | 16 KiB | cache-hot parity |
| active database footprint | 2,48 GiB | 2,48 GiB | 2,48 GiB | 2,48 GiB | 2,48 GiB | 0% |
| files / directories | 364 / 125 | 364 / 125 | 364 / 125 | 364 / 125 | 364 / 125 | 0 |

Первый запуск является cold-cache диагностикой и физически прочитал 852 MiB;
два следующих запуска — сопоставимые cache-hot измерения. Scan throughput по
медиане составил около 8,94 млн rows/s для full row и 39,58 млн rows/s для
projected scan.

## Задержки

Отрицательная дельта означает ускорение. В таблице показаны исходный принятый
100M baseline, непосредственный предыдущий baseline и этот финальный результат.
Stop-порог устойчивой регрессии равен `+25%`.

| Case | Исходный baseline, ms | Предыдущий baseline, ms | Candidate, ms | Delta к предыдущему | Delta к исходному |
|---|---:|---:|---:|---:|---:|
| `server.cold_start_select_database` | 2757,583 | 3090,832 | 3061,895 | -0,9% | +11,0% |
| `correctness.seed_checksum` | 12,112 | 12,352 | 12,497 | +1,2% | +3,2% |
| `select.pk` | 0,483 | 0,483 | 0,481 | -0,4% | -0,4% |
| `select.range` | 0,480 | 0,495 | 0,485 | -2,0% | +1,0% |
| `scan.full` | 88,238 | 98,787 | 93,247 | -5,6% | +5,7% |
| `scan.projected` | 20,865 | 22,381 | 21,055 | -5,9% | +0,9% |
| `aggregate.group_having` | 10,439 | 10,619 | 10,286 | -3,1% | -1,5% |
| `join.parent` | 20,038 | 19,578 | 17,945 | -8,3% | -10,4% |
| `reference.navigation` | 28,771 | 30,143 | 27,313 | -9,4% | -5,1% |
| `reference.direct.explicit` | 20,287 | 20,167 | 18,233 | -9,6% | -10,1% |
| `reference.fact_dictionary` | 682,622 | 717,161 | 725,155 | +1,1% | +6,2% |
| `reference.fact_first` | 415,482 | 429,069 | 415,987 | -3,0% | +0,1% |
| `reference.target_first` | 382,919 | 406,923 | 378,050 | -7,1% | -1,3% |
| `update.rollback` | 50,887 | 54,332 | 44,532 | -18,0% | -12,5% |
| `delete.rollback` | 3,735 | 2,033 | 1,746 | -14,1% | -53,3% |

Ни один case не приблизился к stop-порогу. Единственные замедления к
предыдущему baseline — `correctness.seed_checksum` (+1,2%) и
`reference.fact_dictionary` (+1,1%); оба находятся в шумовом диапазоне.
Access-path sections во всех трёх отчётах byte-identical, SHA-256
`256517746bd0166744b5fdf25286fd9e7ebc91b48fca4bb5245e4627432c32a9`.

## Доказательства

Полные raw-отчёты сохранены вне публичного Git-репозитория. Их локальные пути
на стенде намеренно исключены из опубликованной копии.

Их SHA-256 соответственно:

- `d112fc53aa71bec3ece08bb588b57271282ebf00d66dc4e7d97ed96e63ce478b`;
- `23b7dd23445df28c29cda3f2e4d97b01715237944f42e37594f8984856c2c62e`;
- `5242b929c41387e2cdd23a47553103e10c7ce64b1933d4103c1644da7cfb51cd`.

## Вердикт

Результат принят. На канонической 100M базе финальная crate-структура сохранила
correctness, readback, access paths, footprint и bounded resources; устойчивой
latency/throughput-регрессии относительно двух сохранённых baselines нет.
