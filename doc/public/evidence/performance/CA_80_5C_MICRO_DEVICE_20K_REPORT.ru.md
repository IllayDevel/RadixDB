# CA-80.5c — профиль размера micro-device 20k

[English](CA_80_5C_MICRO_DEVICE_20K_REPORT.md)

Дата: 2026-09-06.

## Вердикт

Профиль `20k` принят как воспроизводимая нижняя Linux resource boundary для
micro-device/minimum-footprint сценария. Он не заменяет канонический профиль
embedded/low-end и слабого железа: им остаётся `10M`. Разделение профилей
намеренное:

- `10M` доказывает практическую работу большой для слабого устройства базы;
- `20k` измеряет минимальную цену процесса, широкой схемы, catalog/artifact
  metadata и базовой DDL/DML/SELECT/recovery-функциональности.

Production-default остаётся `mimalloc`. На constrained fresh-run он быстрее
system allocator на `4,5%`, имеет на `15,1%` меньший final RSS и на `17,3%`
быстрее строит индексы. Для предельно малого чистого runtime system allocator
сохраняется как явная build policy: его clean-reopen median равен `23,4 MiB`
против `54,2 MiB` у `mimalloc`.

Release packaging теперь отделяет DWARF в диагностический artifact. Размер
устанавливаемого `radixdb-server` уменьшен с `74,05 MiB` до `15,56 MiB`, но это
не объявляется уменьшением runtime RSS: машинный код, данные и все ELF
`PT_LOAD` segments у исходного и compact binaries идентичны.

## Кандидат и воспроизводимость

- benchmark source: `1c604d3455056444508ad6fc7de82b3c0a575dd9`;
- split-debug packaging source:
  `7ccc71c637a9a1c25f4955e4643107acdb5ef1a4`;
- version/protocol: `0.5.1` / `13`;
- Cargo.lock SHA-256:
  `a0dfe4bc1679224fadc97f983574a92e50d326c4f3c3ad44d1c9ce550e1dcfbc`;
- host: AMD Ryzen 9 7950X, 32 logical CPUs, Linux
  `7.1.3-201.fc44.x86_64`, NVMe/Btrfs;
- workload: `120` tables, exactly `20 000` rows, `358` secondary/constraint
  indexes;
- constrained configuration: storage workers `1`, page-cache level `0`,
  prefetch/block-cache budgets `0` in benchmark harness;
- fresh run followed by three alternating system/mimalloc clean reopen runs
  with targeted database page-cache eviction;
- explicit bounded page-cache publication followed by another reopen.

Saved benchmark binaries:

| Allocator | `radixdb-bench` SHA-256 |
|---|---|
| system | `257897b343562f925735c4f43da2eaf057aa00b3e58fdbd05e8fcf721a2e3f5a` |
| mimalloc | `cb7311f7dbc30be9b45112f619a88d6d9716419c02076dc3e748ba8b2c1ac3a3` |

## Матрица свежего запуска 20k

Отрицательная delta означает улучшение `mimalloc` относительно system.
`participant.run` включает создание `120` таблиц, загрузку, индексы и query
profile.

| Metric | System | mimalloc | Delta |
|---|---:|---:|---:|
| full participant elapsed | 19 663,122 ms | 18 786,498 ms | -4,5% |
| seed COPY | 313,793 ms | 367,897 ms | +17,2% |
| index build | 5 883,642 ms | 4 867,365 ms | -17,3% |
| peak RSS | 59 588 608 B | 64 684 032 B | +8,6% |
| final RSS | 55 570 432 B | 47 169 536 B | -15,1% |
| final logical storage during run | 9 391 756 B | 9 393 967 B | +0,02% |
| final allocated storage during run | 11 251 712 B | 11 251 712 B | 0% |

Оба run подтвердили checksum `20000:72539880`. Малый seed состоит из `120`
коротких COPY-транзакций, поэтому разница `54,1 ms` не переносится на bulk-load
throughput крупных профилей.

### Почему первый запуск с `mimalloc` занимал около 120 MB

Первичный диагностический run использовал `storage_cpu_workers=auto` и достиг
`39` потоков. Он дал peak/final RSS `137 891 840 / 120 180 736 B`, то есть
`131,5 / 114,6 MiB`. В принятом minimum-footprint профиле с одним storage
worker те же величины равны `61,7 / 45,0 MiB`: сокращение примерно на
`69,8 / 69,6 MiB` соответственно.

Основная измеренная разница этого конкретного сравнения является ценой
параллельного runtime, thread stacks и allocator arenas: ниже отдельный exact
A/B не воспроизвёл десятки мегабайт разницы между unstripped и split-debug
вариантами одного бинарника. Это не универсальное утверждение, что debug layout
никогда не влияет на mappings, faults или поведение конкретного loader/tooling;
оно ограничено данным ELF, host и workload. `workers=auto` остаётся правильным
throughput default, а `workers=1` — осознанным minimum-RSS профилем слабого
устройства.

## Медиана чистого повторного открытия

Каждое значение — median трёх alternating forced-eviction runs на одной и той
же созданной generation.

| Metric | System | mimalloc | Delta |
|---|---:|---:|---:|
| verify elapsed | 268,337 ms | 249,994 ms | -6,8% |
| cold start + select database | 67,561 ms | 50,441 ms | -25,3% |
| peak/final RSS | 24 571 904 B | 56 848 384 B | +131,4% |
| checksum | 12,086 ms | 12,069 ms | -0,1% |
| PK lookup | 0,082 ms | 0,079 ms | -4,3% |
| range | 0,059 ms | 0,059 ms | -0,6% |
| full scan | 0,198 ms | 0,209 ms | +5,7% |
| UPDATE rollback | 8,360 ms | 2,544 ms | -69,6% |
| DELETE rollback | 2,561 ms | 1,856 ms | -27,6% |

Это два разных lifecycle-среза. Fresh final RSS измеряет хвост сразу после
создания схемы, COPY и index build; clean reopen — базовую цену уже созданной
базы. Складывать либо взаимозаменять эти числа нельзя.

## Физический размер

После явного checkpoint и последующего reopen complete database root занимает
`4 509 673 B` logical / `6 422 528 B` allocated. Разбивка:

| Artifact | Files | Logical bytes |
|---|---:|---:|
| `.data` | 120 | 1 321 272 |
| `.idx` | 120 | 224 312 |
| `.cat` | 2 | 1 364 144 |
| `.mft` | 366 | 1 591 584 |
| WAL `.log` | 2 | 162 |
| служебные файлы | 3 | 8 199 |

Это намеренно тяжёлый по metadata fixture: всего `20k` строк распределены по
`120` таблицам. Поэтому результат является boundary широкой схемы, а не
минимальным footprint одной таблицы.

## Контрольная точка и повторное открытие

Отдельный run `checkpoint-mimalloc` принудительно опубликовал bounded page-cache
generation (`state=complete`, target/warmed `1 B`) и сохранил checksum
`20000:72539880`. Следующий forced-eviction run
`reopen-after-checkpoint-mimalloc` снова подтвердил тот же checksum и завершил
полный SELECT/rollback profile. Peak/final RSS составил `60 489 728 B`.

## Компактный исполняемый файл выпуска

Canonical release сохраняет `line-tables-only` в build artifact, затем
`package-artifacts.sh` создаёт компактный installed binary, detached
`debug/radixdb-server.debug`, `.gnu_debuglink` и проверяет общий ELF Build ID.

| Metric | Unstripped | Installed split-debug | Delta |
|---|---:|---:|---:|
| file size | 77 650 160 B | 16 317 472 B | -79,0% |
| `text` | 13 677 059 B | 13 677 059 B | 0% |
| `data` | 365 296 B | 365 296 B | 0% |
| `bss` | 43 009 B | 43 009 B | 0% |
| total `text+data+bss` | 14 085 364 B | 14 085 364 B | 0% |
| detached debug artifact | — | 63 581 840 B | — |

Все четыре `PT_LOAD` segments имеют одинаковые file/memory sizes и flags.
Три alternating запуска пустого constrained server дали median idle RSS
`6 868 KiB` для unstripped и `6 740 KiB` для split-debug, а `VmSize` был
одинаковым — `1 066 264 KiB`. Разница RSS `128 KiB` находится в шуме запуска;
этот idle-срез не обнаружил десятков runtime-мегабайт разницы.

### Точное A/B-сравнение split-debug

После первичного отчёта выполнен дополнительный fresh `20k` A/B именно над
одним и тем же production-allocator benchmark ELF. `objcopy --strip-debug` и
`.gnu_debuglink` сохранили один Build ID и байт-в-байт одинаковые `PT_LOAD`
segments. Три последовательных чередующихся run на отдельных roots дали:

| Metric, median 3 run | Unstripped | Split-debug | Delta split |
|---|---:|---:|---:|
| participant elapsed | 18 651,119 ms | 18 633,892 ms | -0,09% |
| peak RSS | 80 990 208 B | 84 434 944 B | +4,25% |
| final RSS | 60 997 632 B | 61 554 688 B | +0,91% |
| minor faults | 26 599 | 23 802 | -10,52% |
| seed COPY | 359,825 ms | 361,878 ms | +0,57% |
| index build | 4 907,025 ms | 4 862,601 ms | -0,91% |

Диапазоны RSS и elapsed перекрываются; устойчивого runtime RSS выигрыша от
split-debug в этом профиле нет. Проверка не отрицает возможное влияние debug
layout в другой среде, но не позволяет приписать прежнее сокращение примерно
`69,6 MiB` удалению DWARF: exact причинностью для него остаётся
`workers=auto -> workers=1`.

Размер benchmark ELF при этом сократился `95 964 736 -> 19 851 952 B`; detached
debug занимает `78 639 136 B`. Все шесть run подтвердили checksum
`20000:72539880`.

Exact packaged server:

- installed SHA-256:
  `b2e0043c8b6f90d0ef2209db8ee1d4993b1fa4ac8d888674d3debcb8762e86f8`;
- detached debug SHA-256:
  `209c49a8dc386eb72e3069a6c8a7c6b7d501454f42680db92ed449ee875acc43`;
- bundle provenance: clean commit `7ccc71c637a9a1c25f4955e4643107acdb5ef1a4`;
- full release bundle, checksum inventory, systemd dry install/uninstall,
  external backup/restore и tamper rejection прошли.

## Доказательства

Raw benchmark results и saved binaries хранятся вне публичного Git-репозитория.
Их локальные пути намеренно исключены из опубликованной копии. Приведённые
ниже SHA-256 позволяют сверить исходные `REPORT.md`, если они передаются
отдельно.

Fresh `REPORT.md` SHA-256:

- system: `f9341719e9650394af9feb3a5974986c74dda3c02ebdc2d4ea68c0d2c9f8dda4`;
- mimalloc: `51dc19c1a20fe4b9cb80a6fc13cc221c983b7dde0b17be6abc425934849b1eef`.
