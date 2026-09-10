---
title: radixdb-cli
description: Справочник command line для local SQL, обслуживания и logical migration.
---

`radixdb-cli` напрямую открывает databases `memory://` и `file://`. Это не TCP
client для `radixdb-server`. Команда `radixdb-cli --version` фиксирует точную
identity binary, `radixdb-cli --help` выводит встроенную сводку.

## Синопсис

```text
radixdb-cli [OPTIONS]
```

Без action option программа запускает interactive SQL session. Можно выбрать
ровно одно действие из `--execute`, `--file`, `--restore`, `--snapshot`,
`--reset-storage`, `--export-sql` и `--import-sql`.

## Подключение и вывод

| Option | Default | Назначение |
| --- | --- | --- |
| `-d`, `--db DSN` | `memory://` | DSN embedded database |
| `-j`, `--json` | off | Machine-readable result objects в stdout |
| `-q`, `--quiet` | off | Скрыть connection и persistence messages |
| `-l`, `--limit ROWS` | `40` | Сохраняемые display rows; `0` включает предел 100 000 rows |
| `-t`, `--timeout MS` | `0` | Statement timeout; `0` отключает |
| `-h`, `--help` | | Вывести help |
| `-V`, `--version` | | Вывести version и build identity |

JSON result objects сообщают total, retained и omitted rows, а также
truncation head/tail window. Один накопленный CLI statement ограничен 16 MiB.
Для scripts используйте устойчивое сочетание `--quiet --json`.

## SQL actions

| Option | Input | Result |
| --- | --- | --- |
| `-e`, `--execute SQL` | Ровно один statement | Выполнить и завершиться |
| `-f`, `--file PATH` | UTF-8 SQL до 16 MiB | Выполнить statements по порядку и завершиться |
| Без action | Terminal input | Interactive session с history |

```sh
radixdb-cli -q -j -d memory:// --execute "SELECT 42 AS answer"
radixdb-cli -d file:///srv/radixdb/local --file schema.sql
```

Interactive commands: `help`, `\h` или `\?` для help; `exit`, `quit` или `\q`
для выхода. SQL semicolon завершает multi-line input. History хранится в
`~/.radixdb_history`; active transaction откатывается при выходе.

CLI сохраняет `BEGIN ISOLATION LEVEL READ COMMITTED|SNAPSHOT` и направляет
`SAVEPOINT`, `ROLLBACK TO` и `RELEASE` в active embedded transaction.
Unsupported isolation levels отклоняются; ошибка batch откатывает его без
публикации успешного prefix.

## Persistence overrides

Следующие options формируют overrides для `file://` DSN:

| Option | Единица или значения | Default |
| --- | --- | --- |
| `-p`, `--profile PROFILE` | `fast`, `normal`, `durable` | none |
| `-s`, `--sync MODE` | `none`, `normal`, `full` | DSN/default |
| `--checkpoint-interval SECONDS` | seconds, `u32` | `60` |
| `--compact-threshold COUNT` | volumes, `u32` | `4` |
| `--volume-cache-size MB` | MiB, `u32` | `1024` |
| `--wal-max-size MB` | MiB, `u32` | `64` |
| `--compression on\|off` | Boolean | `on` |
| `--keep-snapshots COUNT` | database snapshots, `u32` | `3` |
| `--no-checkpoint-on-close` | flag | checkpoint enabled |

`--sync` и profiles формируют канонический key `sync_mode`. Explicit CLI option
перекрывает соответствующее DSN value, explicit profile сначала задаёт полный
persistence preset, затем применяются individual flags. Banner читает effective
configuration уже открытой базы, поэтому `none`, `normal` и `full` совпадают с
runtime persistence, а не вычисляются второй раз в CLI.

[Указатель конфигурации](../../configuration/) перечисляет все допустимые keys
file DSN. CLI options переопределяют или добавляют только своё подмножество;
unknown и duplicate DSN keys отклоняются.

## Snapshot и reset actions

| Option | Scope | Поведение |
| --- | --- | --- |
| `--snapshot` | `file://` | Создать полный physical database snapshot |
| `--restore [SNAPSHOT_ID]` | `file://` | Атомарно восстановить latest или выбранный 32-hex snapshot |
| `--reset-storage` | `file://` | Разрушительно заменить complete storage generation |

```sh
radixdb-cli -d file:///srv/radixdb/local --snapshot
radixdb-cli -d file:///srv/radixdb/local --restore 0123456789abcdef0123456789abcdef
```

`--snapshot` выводит committed 32-hex snapshot ID, который принимает
`--restore`; retention действует на всю базу. Не запускайте restore или reset,
пока database принадлежит другому process, и сохраняйте внешний backup до любой
destructive operation.

## Logical migration

| Option | Требование |
| --- | --- |
| `--export-sql PATH\|-` | Source database должна читаться этим binary |
| `--import-sql PATH\|-` | Target должен быть несуществующей `file://` database |

Export создаёт checksummed versioned UTF-8 SQL dump из одного consistent
snapshot. Import собирает и закрывает full-sync sibling staging database до
atomic publication target. `-` выбирает stdout или stdin, но file предпочтителен
как сохраняемый cutover artifact.

Любая non-interactive failure возвращает ненулевой process status и пишет
diagnostic в stderr. Операционные процедуры: [«Резервное копирование»](../../../administration/backup-restore/)
и [«Обновление»](../../../administration/upgrading/).
