---
title: Внутреннее устройство хранения
description: Hot MVCC, immutable V6 artifacts, WAL, publication generations и recovery.
---

RadixDB 1.2 сочетает изменяемое MVCC-состояние текущих transactions с immutable
V6 artifacts холодных committed data. Глава объясняет lifecycle и failure
boundaries. Точные binary fields, checksums, limits и crash outcomes остаются
определяются versioned codecs, format constants и исполняемыми recovery tests
в `radixdb-storage`.

## Hot и cold state

```text
transaction -> WAL + hot MVCC versions -> commit visibility
                         |
                         v
                   seal/checkpoint
                         |
          staged .data/.idx + manifests + catalog
                         |
                         v
                 committed CONTROL root
```

Свежие inserts, updates и deletes находятся в version stores и видимы по
правилам MVCC. WAL позволяет восстановить committed changes до их переноса из
hot rows в cold. Checkpoint запечатывает committed rows в immutable segments,
публикует complete physical generation и передвигает replay floor только когда
все охваченные им committed hot rows надёжно записаны в artifacts.

Compaction является независимым maintenance после checkpoint durability. Он
может объединять sub-target segments, удалять obsolete versions или разделять
oversized segment, но не становится неявным владельцем checkpoint или WAL
retention.

## Одна committed generation

Database root содержит следующие классы владельцев:

| Класс | Назначение | Authority |
| --- | --- | --- |
| `CONTROL.0`, `CONTROL.1` | Два фиксированных root slots по 4096 bytes | Выбирают newest completely reachable generation |
| `wal/` | Append-only active и retired generations | Replay DML и transactional catalog mutations после выбранного floor |
| `catalog/` | Immutable typed catalog packs | Names, stable object IDs, dependencies и schema payloads |
| `manifests/` | Immutable manifests database и tables | Связывают catalog, WAL floor и точное membership segments |
| `artifacts/data/` | Immutable `.data` files | Authoritative row IDs, values, statistics и bloom filters |
| `artifacts/index/` | Immutable `.idx` files | Rebuildable exact, ordered и vector accelerators |
| `staging/` | Private complete publication sets | Candidate members до final placement |
| `snapshots/` | Независимо удерживаемые roots | Membership physical backup |
| `quarantine/` | Rename-first garbage collection | Объекты с доказанной недостижимостью до удаления |

Durable membership задаётся только валидным CONTROL graph или snapshot
manifest, но не directory enumeration. Paths выводятся из проверенных
identities; полный root должен находиться на одном filesystem, чтобы final
placement использовал same-filesystem atomic operations.

## Полуколоночные data artifacts

Table manifest перечисляет immutable row или tombstone segments. Каждый
обязательный `.data` artifact хранит row groups с независимо адресуемыми
блоками row ID, columns и bloom. Fixed-width values используют plain blocks;
TEXT, JSON и BYTES могут выбрать более компактное plain или dictionary
представление. Stored blocks могут использовать raw LZ4 compression. Zone maps,
distinct estimates, numeric summaries и bloom filters остаются bounded metadata
внутри `.data`.

Такой layout позволяет scan декодировать выбранные columns и groups без
постоянного разворачивания каждой строки в отдельный object. Metadata-only open
читает directories и statistics без value blocks. Point или range path может
прочитать `.idx` page, а затем соответствующие values из `.data`; full scan
может оставаться column-oriented в подходящих operators и optional TCP
column-batch path.

`.data` является authoritative. Отсутствие или повреждение обязательных data
делает candidate generation недопустимой. `.idx` является accelerator:
недоступный retained index регистрируется, а exact read при возможности
переходит на семантически эквивалентный scan. Повреждённый новый index всё равно
прерывает publication: writer не может зафиксировать заведомо invalid artifact.

## Атомарная публикация

```text
prepare and validate successor graph
        -> write and fsync staged members
        -> move immutable members and fsync parent directories
        -> publish table manifests, catalog, WAL successor, database manifest
        -> replace and fsync the inactive CONTROL slot last
        -> swap the validated runtime generation
        -> retire old WAL and collect unreachable members later
```

Один filesystem publisher связан с canonical root identity, database ID и
удерживаемым OS writer lock. До mutation он отклоняет другой, cloned, moved или
replaced root. Complete staging marker связывает точный candidate member set;
retry может повторно использовать идентичные files, уже перемещённые в final
paths, но не выводит membership из факта их наличия.

Короткий publication fence повторно сверяет source CONTROL до первого final
side effect. Concurrent publishers сериализуются; подготовленный loser получает
stale outcome и не перезаписывает winner. CONTROL является последней durable
commit point. Crash до него оставляет выбранной старую generation, crash после
него оставляет выбранным полный successor.

## Open и recovery

Startup получает writer lock до recovery и независимо проверяет оба CONTROL
slot. Candidates рассматриваются от нового к старому, но победить может только
полностью достижимый и cross-checked graph catalog/manifest/data/WAL. Incomplete
новая generation не скрывает предыдущую complete generation. Одинаковый номер
generation при разных identities означает split-brain corruption и fail closed.

Для candidate recovery сначала проверяет immutable graph и воспроизводит
committed catalog WAL entries после его floor. После выбора одного candidate тот
же WAL owner однократно восстанавливает DML/MVCC state. Incomplete tail
игнорируется; complete corruption или превышение resource limit являются
ошибкой. Recovery никогда не восстанавливает CONTROL или manifests сканированием
filenames.

Readers удерживают выбранную immutable generation, пока она используется query.
GC может убрать member только после доказательства его недостижимости из всех
валидных CONTROL и snapshot roots и отсутствия in-process lease. Операторское
поведение описано в [«Архитектуре хранения»](../../administration/storage/), а
сохраняемые копии в [«Резервном копировании»](../../administration/backup-restore/).
