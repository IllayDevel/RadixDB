---
title: Управление памятью
description: Бюджеты памяти, process RSS, файловый кэш ОС и измеренные профили RadixDB 1.2.
---

RadixDB не загружает всю базу в процесс до начала работы. Cold payloads могут
оставаться на диске и декодируются ограниченными группами, а hot MVCC state,
metadata, рабочая память запросов и выбранное содержимое кэшей занимают память
по необходимости. Настроенные budgets ограничивают отдельных владельцев; их
сумма не является жёстким пределом RSS сервера.

Глава описывает базу документации
`23bf35df011aae6816d77578be96074b02bc363c`. Приведённые измерения памяти
относятся к записанным revisions и workloads, а не автоматически к этому
бинарнику или любой production-нагрузке.

## Владельцы учитываемой памяти

Release configuration предоставляет следующие основные границы:

| Владелец | Release value | Область | Жёсткий предел RSS? |
| --- | ---: | --- | --- |
| Hot state, первый seal trigger | `67108864` bytes | Примерный maintenance trigger на таблицу | Нет |
| Hot state, последующий seal trigger | `16777216` bytes | Примерный trigger на таблицу после появления cold data | Нет |
| Один атомарный `COPY FROM` | `536870912` bytes | Оценка усиления row, MVCC и WAL для одного statement | Нет |
| In-flight protocol frames | `268435456` bytes | Process-wide admission budget | Нет |
| Прогрев файлового кэша ОС | выключен (`page_cache_level = 0`) | На базу, file pages принадлежат ОС | Нет, это не process RSS |

Embedded `PersistenceConfig` дополнительно задаёт default
`volume_cache_bytes = 1073741824`. Он учитывает вытесняемые materialized cold
column payloads, но не row IDs, zone maps, descriptors и другую metadata,
необходимую для routing. Сервер не открывает это значение в `server.toml`; каждая
открытая сервером база получает default движка.

Важно учитывать scope. Per-database values умножаются на число одновременно
открытых баз. Несколько таблиц могут одновременно перейти свои seal triggers, а
несколько sessions — владеть query results, transaction state и cursor buffers.
Allocator arenas, thread stacks, catalog state, indexes, WAL buffers и временная
память compaction не входят в таблицу выше.

## Удалённые cache placeholders

Отдельных cache сжатых блоков и decoded scan prefetch в движке сейчас нет.
Прежние placeholder-параметры `block_cache_bytes` и
`scan_prefetch_cache_bytes` удалены из `PersistenceConfig`, file DSN, server
TOML, release templates и runtime statistics. Любой из этих ключей теперь
fail-closed отклоняется как неизвестный. Конфигурация остаётся честной: будущий
cache сможет вернуться только с настоящими admission, accounting и A/B
доказательствами.

## Файловый кэш операционной системы

Page-cache warmup является отдельным работающим механизмом. Database-local
worker читает members закреплённой immutable generation через повторно
используемый буфер 1 МиБ, а residency и eviction принадлежат ОС. Generation не
копируется в engine-owned heap cache, pages не закрепляются, а корректность не
зависит от их нахождения в памяти.

`page_cache_level` запрашивает десятые доли текущей generation: level 1 — около
10 процентов, level 10 — все bytes generation. Target равен минимуму requested
fraction, доступной памяти за вычетом reserve и положительного
`page_cache_max_bytes`. `page_cache_memory_reserve` задаёт reserve; ноль выбирает
automatic value — большее из 256 МиБ и 10 процентов найденной доступной памяти.
Host `MemAvailable` дополнительно ограничивается текущим cgroup-v2 allowance,
если доступны оба источника. При отсутствии обоих источников automatic warmup
выбирает ноль, а не угадывает.

Сначала прогревается metadata, затем indexes, затем недавно использованные data.
При замене generation неизменившиеся files могут использоваться повторно.
Завершённый warmup означает, что запрошенные bytes прочитаны; после этого ОС
вправе немедленно их вытеснить.

Проверяйте и запускайте warmup вне явной транзакции:

```sql
PRAGMA PAGE_CACHE_STATUS;
PRAGMA PAGE_CACHE_WARMUP;
PRAGMA PAGE_CACHE_WARMUP_WAIT = 300000;
```

Результат содержит одно JSON-значение. Проверяйте `state`,
`total_generation_bytes`, `safe_budget_bytes`, `target_bytes`, `warmed_bytes`,
`resident_estimate_bytes`, `limited_by` и `last_error`. При release level 0
state равен `disabled`, inventory generation не читается.

## Может ли вся база поместиться в кэш?

Нельзя сравнивать bytes базы непосредственно со всей RAM хоста. Сначала получите
размер текущей generation из завершённого `PAGE_CACHE_STATUS`, затем оставьте
память процессу, ОС и пикам workload. Level 10 может охватить generation только
при выполнении условий:

```text
generation_bytes <= page_cache_max_bytes (when nonzero)
generation_bytes <= detected_available_memory - effective_reserve
```

Даже эта оценка не является обещанием. Во время warmup process RSS и файловый
кэш ОС конкурируют за physical memory; другие services и последующие queries
могут вытеснить pages. Snapshots, WAL, staging и superseded generations также
занимают диск и могут добавить file pages, не входящие в current warmup target.

На выделенном хосте начинайте с level 0. Измерьте peak workload RSS и host
`MemAvailable`, выберите explicit reserve больше наблюдавшегося process/system
headroom, затем проверьте небольшой capped level перед увеличением. Не
используйте swap activity или OOM pressure как политику вытеснения кэша.

## Наблюдение за движком и процессом

Engine statistics показывают логических владельцев, а operating system —
процесс и хост:

```sql
PRAGMA VOLUME_STATS;
PRAGMA RUNTIME_STATS;
```

`VOLUME_STATS.memory_bytes` содержит текущую resident storage representation
каждого segment и отдельно показывает metadata, row IDs, index data, descriptors
и column payloads. `RUNTIME_STATS` возвращает суммы hot/cold state, настроенные
cache budgets, page-cache warmup и storage workers. Это bounded snapshot; при
`complete = false` перечислены owners, которые нельзя было прочитать без
блокировки.

В systemd installation отдельно измеряйте процесс и хост. `ps` выводит RSS в
КиБ:

```sh
pid="$(systemctl show radixdb --property MainPID --value)"
ps -o pid=,rss=,vsz=,nlwp=,cmd= -p "$pid"
awk '/MemAvailable|Cached|SwapFree/ { print }' /proc/meminfo
```

Записывайте idle после reopen, peak реального workload и quiescent RSS после
нагрузки. Один финальный sample не показывает кратковременный пик. Для
непрерывного production-наблюдения используйте cgroup или service accounting.

## Сохранённые профили

Следующие результаты намеренно не объединяются:

| Профиль | Identity и условия | Результат |
| --- | --- | --- |
| Малый clean reopen | `1c604d34`, Linux/NVMe, 20 000 строк, 120 таблиц, 358 indexes, один storage worker, page cache и prefetch/block-cache budgets выключены | Median RSS `24571904` B (23,4 МиБ) с system allocator; `56848384` B (54,2 МиБ) с default mimalloc |
| Шестичасовой endurance | `dd0bf75c`, 100 млн seeded rows, 16..256 clients, 1,76 ГиБ RAM, HDD 5400 rpm, конкурирующий I/O и наблюдавшийся SATA reset | Peak server RSS `1060020224` B (1010,9 МиБ); final `206327808` B (196,8 МиБ) |

Малый профиль измеряет reopened database, а не её создание и индексирование.
Шестичасовой профиль использует другую revision и намеренно тяжёлый I/O; это
evidence надёжности, не latency или memory SLA. Ни один результат не включает
bytes OS page cache в process RSS.

## Порядок настройки

1. Сохраните release configuration и запишите baseline idle/reopen.
2. Выполните representative concurrency, query и ingest mix, измеряя RSS,
   cgroup pressure, swap и `RUNTIME_STATS`.
3. Уменьшите число открытых баз и connections до попытки считать отдельные
   cache values глобальным пределом.
4. Если доминирует hot memory, рассматривайте table-level seal thresholds вместе
   с checkpoint и поведением compaction.
5. Включайте page-cache warmup только с explicit reserve и cap, затем сравните
   cold/warm latency с pressure хоста.
6. Меняйте одну группу values за run, перезапускайте сервер и сохраняйте rollback
   configuration.

См. [«Архитектура хранения»](../storage/) о row groups и maintenance и
[«Конфигурация сервера»](../configuration/) о допустимых значениях и restart.
