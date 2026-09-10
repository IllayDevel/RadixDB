---
title: Архитектура хранения
description: Hot MVCC-строки, неизменяемые V6-артефакты, группы строк, сжатие, checkpoint и compaction в RadixDB 1.2.
---

RadixDB 1.2 использует гибридную модель хранения. Последние изменения находятся
в изменяемом MVCC-состоянии; checkpoint запечатывает зафиксированные строки в
неизменяемые DATA- и INDEX-артефакты с контрольными суммами. Запрос объединяет
обе части в одном снимке транзакции. Приложение по-прежнему читает и изменяет
SQL-таблицы, а не выбирает строчное или колоночное хранилище.

Глава описывает реализацию V6 из базы исходников RadixDB 1.2
`23bf35df011aae6816d77578be96074b02bc363c`. Контракт снятого с поддержки
формата 0.5.x сюда не переносится.

## Hot- и cold-состояние

Две части хранилища решают разные задачи:

| Часть | Представление | Назначение |
| --- | --- | --- |
| Hot | Изменяемые MVCC-строки и версии транзакций | Вставка, обновление, удаление и snapshot visibility с малой задержкой |
| Cold | Неизменяемые V6 DATA-артефакты и INDEX-артефакты | Компактное долговременное хранение, ограниченное чтение и колоночные scans |

Зафиксированная hot-строка остаётся видимой во время seal. Publication атомарно
переключает долговечную физическую generation; читатель закрепляет generation и
не видит частично заменённый набор артефактов. Tombstones и hot-версии
объединяются с cold-строками, поэтому scan не возвращает вытесненные данные.

Не открывайте одну file database одновременно через сервер и embedded process.
У её WAL, manifests и generations должен быть один владелец.

## Seal и checkpoint

Server release начинает первый seal примерно при `67108864` оценочных hot bytes
на таблицу, последующие incremental seals для таблицы с cold segments — примерно
при `16777216` bytes. Эти thresholds запускают обслуживание, но не являются
ограничениями числа строк или жёсткими пределами памяти процесса.

`target_volume_rows = 1048576` задаёт форму новых результатов seal и compaction.
Writer выравнивает результат по группам из 65 536 строк, поэтому release target
обычно содержит около 16 полных групп. Последняя группа может быть короче.
Изменение target влияет на новые outputs, а не на опубликованные артефакты.

Checkpoint согласует seal hot-строк, состояние catalog, manifests и продвижение
WAL. Если эксплуатационной процедуре нужна известная долговечная generation,
запросите её через SQL:

```sql
PRAGMA CHECKPOINT;
PRAGMA VOLUME_STATS;
```

При штатной остановке default persistence configuration также выполняет
финальный checkpoint. Успешный checkpoint сам по себе не является backup;
допустимый способ копирования согласованной generation описывает отдельная глава.

## Неизменяемые V6-артефакты

Каждый cold-сегмент таблицы представлен неизменяемым DATA-артефактом и, когда
нужно, INDEX-артефактами. Catalog и database/table manifests определяют
зафиксированную generation и её состав. Контрольные суммы защищают headers,
directories и stored payloads; reader отклоняет неверные границы, неизвестные
tags, несовпадение checksums и противоречивые cross-references.

Имена и каталоги артефактов принадлежат движку. Не переименовывайте, не заменяйте,
не копируйте и не удаляйте отдельные файлы открытой базы. Directory listing не
является протоколом согласованности: рядом с выбранной generation могут находиться
staged, superseded и snapshot files.

## Группы строк и колонки

DATA-артефакт делит строки на группы размером не более 65 536. В каждой группе
есть блок row IDs, по одному блоку каждой хранимой колонки и необязательные
Bloom-filter blocks. Directories описывают колонки, группы, блоки и статистику до
выделения памяти и декодирования payload.

Значения фиксированной ширины имеют каноническое little-endian представление.
Блоки переменной ширины `TEXT`, `JSON` и `BYTES` могут использовать plain или
dictionary layout; writer выбирает меньший stored candidate для каждого блока.
Validity bitmap представляет NULL независимо от bytes значения.

Статистика группы и Bloom data позволяют исключать неподходящие группы.
Projected scan может декодировать только выбранные колонки. Поэтому cold path
является колоночным, а hot transactional state остаётся строчным. Из этого не
следует наличие spatial indexes или всех access methods PostgreSQL.

## Сжатие

Default persistence profile включает raw-block LZ4 для cold DATA-артефактов.
Формат также допускает несжатые блоки. При распаковке проверяется заявленная
logical length, а неверные ratio и bounds отклоняются до выделения результата;
checksum покрывает stored bytes.

Server TOML 1.2 не предоставляет переключатель сжатия. Embedded file DSN
принимает `volume_compression`. Прежний placeholder `compression_threshold`
удалён, потому что seal и compaction не реализовывали обещанное им поведение;
теперь такой ключ fail-closed отклоняется как неизвестный. У compression остаётся
один честный контракт: включить или выключить его для новых cold volumes.

Коэффициент сжатия зависит от типов, cardinality и распределения значений.
Размер deployment определяйте по representative load с checkpoint и reopen, а
не по универсальному коэффициенту.

## Compaction

Seals создают неизменяемые level-zero segments. Background compaction объединяет
подходящие segments, применяет tombstones и публикует заменяющие артефакты.
Одна job владеет одной таблицей; release выполняет одну job одновременно.
Input/output budgets, disk reserve и level-zero backpressure защищают
publication, хотя server TOML сейчас открывает только часть этих настроек.

Больший `target_volume_rows` может уменьшить metadata на volume и улучшить
сжатие, но увеличить объём перезаписи. Меньшие outputs сокращают одну перезапись
ценой большего числа артефактов и metadata. Сохраняйте release value до измерения
полного ingest, scan и compaction cycle на целевой filesystem.

## Наблюдение за хранилищем

`PRAGMA VOLUME_STATS` возвращает строку для каждого cold segment: tier, число
строк, компоненты resident memory, idle cycles и tombstones. `PRAGMA
RUNTIME_STATS` возвращает одно JSON-значение с общими hot/cold counters и
состоянием maintenance. Это текущий снимок, который может измениться сразу после
запроса.

```sql
PRAGMA VOLUME_STATS;
PRAGMA RUNTIME_STATS;
```

Метод server protocol `database_status(name)` сообщает полноту filesystem
inventory и число WAL, artifact, snapshot, checkpoint и manifest files. Он не
возвращает их размер. Для capacity planning измеряйте полный database root после
checkpoint и отдельно оставляйте место для WAL, snapshots, staging, compaction
output и filesystem.

## Измеренный объём

В сохранённом сравнении 100 млн строк на revision `b648b2d3` одна база RadixDB
заняла `1964220021` logical bytes, соответствующая база PostgreSQL 18.3 —
`16126596799` bytes. Использовался фиксированный relational fixture, Ryzen 9
7950X, NVMe и Btrfs; page-cache warmup был выключен. Разница примерно в 8,21
раза относится только к этому dataset, не является общей гарантией сжатия и не
является замером `23bf35df`.

Глава [«Память»](../memory/) разделяет disk footprint, process RSS и файловый
кэш ОС. [«Конфигурация сервера»](../configuration/) определяет настройки,
которые принимает `server.toml`. Не пытайтесь собирать backup из отдельных
артефактов, пока специальная глава о backup и восстановлении не опубликована.
