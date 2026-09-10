---
title: Обновление RadixDB
description: Перенос базы на новую версию RadixDB без использования физических файлов как интерфейса совместимости.
---

При обновлении меняется не только исполняемый файл. У wire-протокола сервера,
поведения SQL, схемы конфигурации, физического формата хранения и логического
дампа разные границы совместимости. До изменения production-сервиса нужно
проверить каждую границу для целевой версии.

Эта процедура проверена на базе документации
`40b1b3d13e050afa2666a0414b7215d5ac1452c0`. База использует wire-протокол 17
и строгий физический reader V6. В ней нет in-place миграции физических файлов,
а устаревшие поколения хранения не открываются через compatibility defaults.

## Границы совместимости

| Граница | Правило обновления |
| --- | --- |
| Сервер и клиент | Build identity и wire-протокол должны быть совместимы; protocol 17 отклоняет несовместимый handshake |
| SQL и приложение | Выполнить запросы и инварианты приложения на кандидате; одного разбора синтаксиса недостаточно |
| Конфигурация | Проверить полный кандидат `server.toml`; неизвестные и неверные значения запрещают старт, изменения требуют restart |
| Native extensions | Сохранить каждый exact package, fingerprint и codec, требуемый базами; packages не входят в physical backup |
| Физическая база | Открывать только версией с явно заявленной поддержкой формата; in-place конвертера на этой базе нет |
| Логический SQL-дамп | Экспортировать старым бинарником, импортировать новым в отсутствующий root, затем проверить и повторно экспортировать |
| Резервная копия | Сначала доказать, что архивный исходный бинарник восстанавливает и открывает её; автоматической forward compatibility нет |

Нельзя заменять только сервер, оставляя непроверенный клиент, или только CLI,
который нужен для восстановления. Храните каждый проверенный по checksum release
bundle вместе с build provenance, шаблоном конфигурации, CLI и recovery scripts.

## Граница native extensions

До замены server соберите inventory extension bindings из `DESCRIBE DATABASE`
и архивируйте каждый совпадающий полный package. Добавление более высокого
SemVer с тем же package UUID делает эту version active при следующем startup,
но не обновляет существующий catalog binding. Database, закрепленная за старой
version, откроется в restricted diagnostic mode, если старый exact package
перестал быть active.

В RadixDB 1.2 нет `ALTER EXTENSION UPDATE`, hot reload или automatic codec
migration. Сохраняйте старый package для rollback, а при смене identity или
codec выполняйте explicit logical либо application migration в отдельно
проверенные objects. Compatibility report команды
`cargo radixdb-plugin package --previous-package` является release evidence, а
не catalog migration. См. [«Extensions»](../extensions/).

## Подготовка перехода

До остановки запишите обе версии, используемые пути и свободное место. Пути
должны быть абсолютными. Старый и новый root должны отличаться, новый root не
должен существовать.

```sh
OLD_BUNDLE=/srv/radixdb-releases/old
NEW_BUNDLE=/srv/radixdb-releases/1.2
OLD_ROOT=/opt/radixdb/data/databases/app
NEW_ROOT=/opt/radixdb/data/databases/app-v11
DUMP=/srv/radixdb-migrations/app-v11.sql

"$OLD_BUNDLE/bin/radixdb-cli" --version
"$NEW_BUNDLE/bin/radixdb-cli" --version
test -d "$OLD_ROOT"
test ! -e "$NEW_ROOT"
df -B1 "$OLD_ROOT" "$(dirname "$NEW_ROOT")" "$(dirname "$DUMP")"
```

До окна обслуживания:

1. Проверьте external physical restore старым bundle.
2. Проведите пробную логическую миграцию на копии и запишите fingerprints строк,
   constraints, indexes, views и прикладных данных.
3. Проверьте конфигурацию кандидата и протокол клиента в отдельном сервисе.
4. Оцените место для dump, нового физического root и rollback-копии.
5. Определите момент остановки записи приложения. RadixDB 1.2 не предоставляет
   replication, online logical catch-up или automatic failover для перехода.

## Экспорт старым движком

Остановите запись приложения и сервер. Убедитесь, что ни один embedded process
не удерживает lock базы. Экспорт выполняет старый CLI: именно он является
авторитетным reader старых физических файлов.

```sh
sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
test ! -e "$DUMP"
sudo -u radixdb "$OLD_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$OLD_ROOT?checkpoint_on_close=off" \
  --export-sql "$DUMP"
sha256sum "$DUMP" > "$DUMP.sha256"
```

Dump представляет собой версионированный детерминированный SQL stream с
идентификатором источника, schema fingerprint, счётчиками и checksum trailer.
Экспорт читает один MVCC snapshot и публикует output атомарно. Старый физический
root оставьте неизменным и offline: до конца приёмки это источник rollback.

При получении exclusive ownership обновляется owner record в `LOCK`. Проверенный
экспорт оставляет остальные durable artifacts источника побайтово неизменными;
поэтому сравнивайте source data и reachable artifacts, а не transient lock
record.

Проходят малый oracle логической миграции, исполняемый roundtrip документации и
messenger-shaped rehearsal. Последний создаёт три побайтово одинаковых export,
проверяет reviewed digest логического содержимого после нормализации только
строки source-version provenance, удаляет source root, импортирует в отсутствующий
target, проверяет все 18 tables, 8 views и 37 rows, затем reopen и re-export
target. Для реального cross-version перехода это всё равно не заменяет rehearsal
точной пары source/target binaries и прикладной схемы.

Никогда не копируйте старые CONTROL, WAL, catalog, manifests, DATA, INDEX,
snapshots или retired volumes в целевой root. WAL служит для восстановления
своего физического поколения, а не для переноса между версиями.

## Импорт в отсутствующий root

Новый CLI проверяет stream, создаёт соседнюю staging database в режиме full-sync,
исполняет dump, делает checkpoint и close и только после этого публикует target
через `RENAME_NOREPLACE`.

```sh
sha256sum -c "$DUMP.sha256"
test ! -e "$NEW_ROOT"
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$NEW_ROOT" \
  --import-sql "$DUMP"
test -d "$NEW_ROOT"
```

Ошибка checksum, SQL, checkpoint или close оставляет запрошенный target
отсутствующим. Существующий target отклоняется; нельзя очищать его ради успешной
команды. Сохраните ошибочный dump и логи, для следующей попытки выберите новый
target.

## Проверка до переключения

Сначала запросите новый root новым CLI в offline-режиме. Проверяйте не только
counts: primary и unique constraints, foreign-key behavior, планы secondary
indexes, views, NULL, последние подтверждённые строки и прикладные aggregates.

```sh
REEXPORT=/srv/radixdb-migrations/app-v11.reexport.sql
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$NEW_ROOT?checkpoint_on_close=off" \
  --execute "SELECT COUNT(*) AS rows FROM critical_table"
sudo -u radixdb "$NEW_BUNDLE/bin/radixdb-cli" --quiet \
  --db "file://$NEW_ROOT?checkpoint_on_close=off" \
  --export-sql "$REEXPORT"
cmp --silent "$DUMP" "$REEXPORT"
```

Точный re-export является сильной проверкой transport для версий с общим dump
contract, но не заменяет прикладные тесты. Если целевая версия объявляет новую
ревизию logical dump, сравнивайте заявленные counts и canonical application
fingerprints, а не рассчитывайте на побайтовое равенство.

Направьте отдельный сервер-кандидат на data directory, где новый root находится
под окончательным именем базы. Выберите её, дождитесь ready от
`database_status(name)` и выполните характерные read/write/rollback операции.
После переключения приложения наблюдайте ошибки, latency, WAL, compaction и
заполнение диска в течение окна приёмки.

## Граница отката

До первой записи новой версией можно вернуться к нетронутому старому root и
старому bundle. После начала новых записей такой переход потеряет их, если у
приложения нет отдельно проверенного пути reconciliation. Старый бинарник нельзя
открывать новый физический root даже для попытки rollback.

Храните старый root, dump, checksums, старый bundle и pre-upgrade external backup
до завершения приёмки и срока rollback retention. Успешный импорт не является
основанием удалить единственную восстанавливаемую копию старого формата.

Перед репетицией перехода прочитайте [резервное копирование](../backup-restore/),
а сигналы приёмки кандидата описаны в главе [мониторинг](../monitoring/).
