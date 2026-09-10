---
title: Ограничения
description: Структурные пределы, настраиваемые ограничения и измеренный масштаб RadixDB 1.2.
---

RadixDB не публикует одно максимальное число строк или один максимальный размер
базы. Ёмкость ограничена несколькими независимыми пределами формата,
настройками допуска, доступной памятью, файловыми дескрипторами и хранилищем.
Произведение наибольших значений ниже не является поддерживаемым размером
развёртывания.

В этом приложении разделены три вида доказательств:

- **Жёсткий предел**: ограничение формата или семантики, проверяемое кодом 1.2.
- **Настраиваемое ограничение**: управляемый оператором допуск или ресурс.
- **Измеренный масштаб**: завершённая нагрузка в записанных условиях, а не
  жёсткий предел, обещание ёмкости или SLA задержки.

Если строка не говорит обратного, пределы сверены на
`23bf35df011aae6816d77578be96074b02bc363c`. MiB и GiB используют степени 1024.
Полная поверхность настройки приведена в [справочнике конфигурации](../../reference/configuration/).

## Пределы каталога и семантики

| ID | Ограничение | Значение и единица | Условие применения | Источник и SHA |
| --- | --- | ---: | --- | --- |
| HARD-01 | Файл каталога | 536 870 912 байт (512 MiB) | Больший catalog pack отклоняется до декодирования | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-02 | Объекты одного каталога | 262 144 объекта | Для одного декодированного поколения каталога | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-03 | Рёбра одного каталога | 1 048 576 рёбер | Для рёбер зависимостей каталога | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-04 | Поля одного объекта каталога | 64 поля | Кодек объекта отклоняет больший каталог полей | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-05 | Payload одного объекта каталога | 16 777 216 байт (16 MiB) | Кодированный payload объекта, не размер всего каталога | `crates/radixdb-catalog/src/codec/primitives.rs` @ `23bf35df` |
| HARD-06 | Нормализованное имя | 1 024 байта | Длина UTF-8 после нормализации | `crates/radixdb-catalog/src/name.rs` @ `23bf35df` |
| HARD-07 | Отображаемое имя | 4 096 байт | Длина UTF-8 сохранённого написания | `crates/radixdb-catalog/src/name.rs` @ `23bf35df` |
| HARD-08 | Канонический текст SQL | 16 777 216 байт (16 MiB) | На одно принадлежащее каталогу SQL-значение | `crates/radixdb-catalog/src/payload/common.rs` @ `23bf35df` |
| HARD-09 | Глубина зависимости каталога | 256 рёбер | Более глубокий граф отклоняется при валидации | `crates/radixdb-catalog/src/graph/validate.rs` @ `23bf35df` |
| HARD-10 | Аргументы процедуры | 1 024 аргумента | На одно сохранённое определение процедуры | `crates/radixdb-catalog/src/payload/routine.rs` @ `23bf35df` |
| HARD-11 | Столбцы результата процедуры | 4 096 столбцов | На одно сохранённое определение результата | `crates/radixdb-catalog/src/payload/routine.rs` @ `23bf35df` |
| HARD-12 | Аргументы задания | 1 024 аргумента | На одно сохранённое определение задания | `crates/radixdb-catalog/src/payload/job.rs` @ `23bf35df` |
| HARD-13 | Один литерал задания | 8 388 608 байт (8 MiB) | На кодированное значение аргумента задания | `crates/radixdb-catalog/src/payload/job.rs` @ `23bf35df` |
| HARD-14 | Размерность вектора | 65 535 измерений | На объявление векторного типа в каталоге | `crates/radixdb-catalog/src/payload/data_type.rs` @ `23bf35df` |
| HARD-15 | Глубина навигации по ссылкам | 8 шагов | На один развёрнутый путь read-only запроса | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |
| HARD-16 | Пути навигации по ссылкам | 256 путей | На развёртывание одного запроса | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |
| HARD-17 | Рёбра навигации по ссылкам | 512 рёбер | По всем развёрнутым путям одного запроса | `crates/radixdb-executor/src/navigation/mod.rs` @ `23bf35df` |

Эти пределы описывают допускаемые определения и развёртывание запросов. Они не
означают, что схема одновременно вблизи всех пределов поместится в доступную
процессу память.

## Пределы native extensions

Эти limits принадлежат plugin ABI 1.0 и startup loader 1.2. Они не являются
разрешением приблизить один package сразу ко всем максимумам.

| ID | Предел | Значение и единица | Условие enforcement | Source и SHA |
| --- | --- | ---: | --- | --- |
| PLUG-01 | Package manifest | 65 536 байт (64 KiB) | Проверяется до TOML decode | `crates/radixdb-plugin-host/src/manifest.rs` @ `40b1b3d1` |
| PLUG-02 | Package shared library | 268 435 456 байт (256 MiB) | Пустой или больший `.so` отклоняется | `crates/radixdb-plugin-host/src/manifest.rs` @ `40b1b3d1` |
| PLUG-03 | Stable local ID | 255 байт | UTF-8, непустой и без NUL | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-04 | Одно external value | 16 777 216 байт (16 MiB) | Type descriptor может задать меньший `max_bytes` | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-05 | Entries одной descriptor table | 65 535 entries | Применяется независимо к каждому descriptor array | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-06 | Arguments native function | 1 024 аргумента | На одну descriptor signature | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-07 | Planner candidate spans | 4 096 spans | Support descriptor может объявить меньший maximum | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-08 | Hash components | 256 components и 65 536 байт | На один semantic hash callback | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |
| PLUG-09 | Plugin diagnostic detail | 4 096 байт | Более длинная UTF-8 detail ограничивается до host mapping | `crates/radixdb-plugin-abi/src/constants.rs` @ `40b1b3d1` |

Начальный package ABI принимает только x86-64 little-endian ELF для
`x86_64-unknown-linux-gnu` с glibc не новее 2.36. Это platform boundary, а не
предел bytes. Полный admission contract приведен в
[«Установке и эксплуатации extensions»](../../administration/extensions/).

## Пределы формата хранения

| ID | Ограничение | Значение и единица | Условие применения | Источник и SHA |
| --- | --- | ---: | --- | --- |
| HARD-18 | Запись CONTROL | 4 096 байт | Фиксированный размер записи, не настройка памяти | `crates/radixdb-storage/src/v6/control.rs` @ `23bf35df` |
| HARD-19 | Таблицы в манифесте базы | 262 144 таблицы | На одно поколение базы | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-20 | Сегменты в манифесте таблицы | 1 048 576 сегментов | На одно поколение таблицы | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-21 | Манифесты при открытии | 262 145 манифестов | Один манифест базы и манифесты таблиц | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-22 | Сегменты при открытии | 4 194 304 сегмента | Сумма по открытому поколению базы | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-23 | Metadata при открытии | 536 870 912 байт (512 MiB) | Учтённые декодированные metadata одного поколения | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-24 | Достижимые идентификаторы | 8 388 608 идентификаторов | Узлы при валидации одного поколения | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-25 | Работа проверки достижимости | 1 073 741 824 байта (1 GiB) | Учтённый бюджет валидации достижимости | `crates/radixdb-storage/src/v6/reachability.rs` @ `23bf35df` |
| HARD-26 | Один immutable artifact | 68 719 476 736 байт (64 GiB) | Предел файловой раскладки DATA или INDEX | `crates/radixdb-storage/src/v6/artifact.rs` @ `23bf35df` |
| HARD-27 | Строки одного DATA artifact | 4 294 967 295 строк | `u32::MAX`; это не предел таблицы или базы | `crates/radixdb-storage/src/v6/manifest/model.rs` @ `23bf35df` |
| HARD-28 | Столбцы одного табличного artifact | 4 096 столбцов | Проверяется в заголовке DATA | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-29 | Row groups одного DATA artifact | 65 536 групп | Проверяется в заголовке DATA | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-30 | Строки одной row group | 65 536 строк | Проверка ёмкости DATA row groups | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-31 | Блоки одного DATA artifact | 4 194 304 блока | Предел каталога DATA | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-32 | Stored bytes одного DATA block | 268 435 456 байт (256 MiB) | Сжатый дисковый payload блока | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-33 | Logical bytes одного DATA block | 536 870 912 байт (512 MiB) | Декодированный payload блока | `crates/radixdb-storage/src/v6/data/model.rs` @ `23bf35df` |
| HARD-34 | Одно значение переменной длины | 268 435 456 байт (256 MiB) | На значение перед кодированием DATA | `crates/radixdb-storage/src/v6/data/column.rs` @ `23bf35df` |
| HARD-35 | Accelerators одного INDEX artifact | 4 096 accelerators | Exact, ordered и vector структуры делят один каталог | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-36 | Sections одного INDEX artifact | 16 384 sections | Предел каталога INDEX | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-37 | Pages одного INDEX artifact | 4 194 304 pages | Сумма по accelerators | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-38 | Ключевые столбцы одного индекса | 64 столбца | На один index accelerator | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-39 | Entries одной INDEX page | 1 048 576 entries | Число записей также ограничено байтами страницы | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-40 | Stored bytes одной INDEX page | 67 108 864 байта (64 MiB) | Сжатый дисковый payload страницы | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-41 | Logical bytes одной INDEX page | 268 435 456 байт (256 MiB) | Декодированный payload страницы | `crates/radixdb-storage/src/v6/index/model.rs` @ `23bf35df` |
| HARD-42 | Catalog WAL replay bytes | 1 073 741 824 байта (1 GiB) | На один допуск recovery replay | `crates/radixdb-storage/src/v6/catalog_wal.rs` @ `23bf35df` |
| HARD-43 | Транзакции catalog WAL replay | 262 144 транзакции | На один допуск recovery replay | `crates/radixdb-storage/src/v6/catalog_wal.rs` @ `23bf35df` |
| HARD-44 | Элементы одного snapshot | 8 388 608 элементов | На один snapshot manifest | `crates/radixdb-storage/src/v6/snapshot/model.rs` @ `23bf35df` |
| HARD-45 | Snapshot manifest | 805 306 672 байта | Предел кодированного manifest для максимального каталога элементов | `crates/radixdb-storage/src/v6/snapshot/model.rs` @ `23bf35df` |

Предел artifact защищает декодирование и выделение памяти. Обычные рабочие
цели намеренно меньше и формируются настройками sealing, compaction и cache.

## Настраиваемые ограничения runtime

Это ориентированная на ёмкость выборка из `release/server.toml`, а не второй
справочник конфигурации. Сервер читает эти параметры при запуске.

| ID | Параметр | Значение release | Допустимая граница или смысл | Источник и SHA |
| --- | --- | ---: | --- | --- |
| CFG-01 | `max_connections` | 64 подключения | Положительный process admission; default кода при отсутствии равен 151 | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-02 | `max_inflight_frame_bytes` | 268 435 456 байт (256 MiB) | Положительный общий бюджет frame payloads процесса | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-03 | `max_databases` | 64 базы | Положительное число записей реестра баз процесса | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-04 | `max_database_name_bytes` | 64 байта | Положительный допуск длины UTF-8 выбранного имени | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-05 | `cursor_batch_max_rows` | 1 024 строки | Положительное число строк в одном ответе cursor | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-06 | `cursor_batch_max_bytes` | 8 388 608 байт (8 MiB) | Положителен и не больше `max_frame_bytes` | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-07 | `max_frame_bytes` | 67 108 864 байта (64 MiB) | Не меньше 256 байт и не больше in-flight budget | `release/server.toml`, `crates/radixdb-protocol/src/lib.rs`, `src/server/config.rs` @ `23bf35df` |
| CFG-08 | `copy_max_transaction_bytes` | 536 870 912 байт (512 MiB) | Положительный memory envelope одного атомарного `COPY FROM` | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-09 | `max_compaction_jobs` | 1 задание | Допустимый диапазон от 1 до 8 заданий | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-10 | `storage_cpu_workers` | 0 workers | Ноль включает автоматизм по видимым host/cgroup CPU | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-11 | `page_cache_level` | 0 | Диапазон от 0 до 10; ноль выключает proactive warmup | `release/server.toml`, `crates/radixdb-storage/src/config.rs` @ `23bf35df` |
| CFG-12 | `target_volume_rows` | 1 048 576 строк | Минимум 65 536 строк; задаёт форму новых cold volumes | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |
| CFG-13 | `read_queue_depth` | 1 запрос | Положителен; release сохраняет переносимое последовательное чтение | `release/server.toml`, `src/server/config.rs` @ `23bf35df` |

Эти байтовые бюджеты не являются пределом общего RSS. Одновременные запросы,
декодированные значения, allocator arenas, стеки потоков, metadata и page cache
операционной системы остаются отдельными потребителями. Уменьшение лимита может
отклонить работу, которая иначе поместилась бы; увеличение требует проверки на
целевой нагрузке и сценариях отказа.

## Измеренный масштаб

Следующие строки сохраняют свидетельства именованных экспериментов. Их исходные
ревизии предшествуют базе документации, поэтому это достигнутый масштаб линии
разработки V6, а не свежее измерение бинарника `23bf35df`.

| ID | Нагрузка и результат | Условия | Идентичность evidence |
| --- | --- | --- | --- |
| SCALE-01 | 20 000 строк, 120 таблиц и 358 индексов; median peak/final RSS чистого reopen равен 24 571 904 байтам с system allocator и 56 848 384 байтам с mimalloc | Один storage worker, page-cache level 0, принудительное вытеснение файлов базы; AMD Ryzen 9 7950X, NVMe/Btrfs, Linux | [Отчёт 20k](../../../evidence/performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.ru.md); engine `1c604d3455056444508ad6fc7de82b3c0a575dd9` |
| SCALE-02 | 100 000 000 строк; active database 1 964 220 021 байт; median трёх verify runs: peak RSS 547 303 424 байта и final RSS 293 380 096 байт | Page-cache level 0; AMD Ryzen 9 7950X, Apacer AS2280Q4U NVMe, Btrfs; один evicted и два cache-hot verify runs | [Отчёт 100M](../../../evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.ru.md); engine `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc` |
| SCALE-03 | Нагрузка 21 600 000 ms со 100 000 000 активных строк и лестницей 16/32/64/128/256 клиентов; peak server RSS 1 060 020 224 байта, final RSS 206 327 808 байт | Intel Celeron 847, RAM 1,76 GiB, HDD 5400 rpm, ext4; побочный I/O и реальный ATA reset; latency явно не является SLA | [Шестичасовой отчёт](../../../evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.ru.md); engine `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |

Полная методика и сравнения производительности будут собраны в приложении после
его публикации. Границы поддержки SQL смотрите в
[матрице совместимости](../compatibility/).

## Решения по ёмкости

Перед production проверяйте предполагаемую схему и конкурентность с теми же
allocator, файловой системой, классом хранилища и настройками durability.
Записывайте steady state и пики во время import, построения индексов,
compaction, checkpoint, snapshot и reopen. Успех меньшего профиля нельзя
экстраполировать как подтверждение большего.
