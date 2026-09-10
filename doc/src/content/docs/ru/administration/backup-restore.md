---
title: Резервное копирование и восстановление
description: Создание согласованной резервной копии RadixDB 1.2 вне каталога базы и восстановление в новый каталог.
---

Каталог базы, который открывается после restart, ещё не является резервной
копией. Backup должен представлять определённую точку восстановления, переживать
потерю исходного носителя, иметь inventory целостности и проходить проверочное
восстановление до того, как станет нужен.

Глава описывает physical backup на базе документации
`23bf35df011aae6816d77578be96074b02bc363c`. Поддерживаемая release-процедура
использует `backup-external.sh` и `restore-external.sh` из того же проверенного
bundle, что и CLI. Она обрабатывает по одной database root и восстанавливает
только в ранее отсутствовавший каталог.

## Выбор артефакта

| Артефакт | Расположение и назначение | Граница восстановления |
| --- | --- | --- |
| Physical snapshot | `snapshots/<32-hex-id>/` внутри database root; локальный rollback и источник внешнего backup | Одна catalog/data/index generation и требуемый непрерывный WAL suffix |
| Внешний physical backup | Read-only копия retained snapshot tree, `BACKUP.env` и `SHA256SUMS` вне database root | Тот же physical format; сначала восстанавливать matching release |
| Логический SQL dump | Версионированный SQL stream от `--export-sql` | Схема и строки для миграции в release, не читающий старый physical format |

Внутренний snapshot остаётся на той же файловой системе, что и база. Он не
переживёт потерю этой файловой системы и не является независимым backup.
Checkpoint также не является backup: он публикует новую storage generation и
продвигает WAL retention, но не создаёт внешний recovery artifact.

Не копируйте работающую database root через `cp`, `rsync` или файловый архиватор.
Скопированные CONTROL, manifests, artifacts и WAL могут относиться к разным
publication boundaries. Используйте engine snapshot path или отдельно
интегрированную и проверенную с RadixDB процедуру storage snapshot.

### Databases с native extensions

Bundled CLI и `backup-external.sh` сейчас не принимают allowlist native
packages. Поэтому они открывают базу с empty plugin registry и не могут создать
или проверить backup database, связанной с extension. То же ограничение
относится к описанной процедуре CLI logical export/import.

Не считайте эти wrappers доказательством восстановимости extension-bound data.
Сохраняйте exact complete packages отдельно и до production эксплуатации
создайте независимо проверенную процедуру backup и restore. Копирование live
files или отключение exact package admission не является заменой. Обычная
процедура ниже применима к databases без extension binding.

## Согласованность snapshot

`PRAGMA SNAPSHOT` допустим только для persistent database и вне explicit
transaction:

```sql
PRAGMA SNAPSHOT;
```

Движок кратко фиксирует одну commit boundary в порядке locks checkpoint,
закрепляет опубликованную physical generation и замораживает все WAL generations
от её replay floor. Затем он копирует достижимые catalog, table manifests, DATA-
и INDEX-artifacts и необходимый WAL suffix. Для каждого member заданы длина и
SHA-256. `SNAPSHOT.mft` синхронизируется и публикуется последним; каталог без
этого final manifest не является committed snapshot.

После фиксации boundary обычные commits могут продолжиться, пока snapshot
копирует immutable members. DDL, checkpoint и compaction остаются ограждены
дольше. Cancellation возвращает ошибку, а незавершённая snapshot work не
является точкой восстановления.

Явный checkpoint перед snapshot не требуется для согласованности. Committed hot
rows после replay floor переносятся WAL. Checkpoint может уменьшить WAL suffix и
сделать размер backup и работу restore предсказуемее, но добавляет собственный
I/O и publication cycle.

## Подготовка внешней копии

Комплектный скрипт открывает file database через `radixdb-cli`, поэтому
эксклюзивный `LOCK` базы должен быть свободен. Для server installation штатно
остановите весь service. Это не даст новому клиенту открыть ранее не открытую
базу во время backup.

Сохраните полный checksum-verified release bundle: systemd installer копирует
CLI в `/opt/radixdb/bin`, но не устанавливает backup scripts. Родитель backup
должен существовать, быть writable для `radixdb` и находиться вне database root.
Заранее обеспечьте место для нового внутреннего snapshot и его внешней копии;
фиксированного коэффициента нет, потому что объём indexes и WAL suffix зависит
от нагрузки.

```sh
BUNDLE=/srv/radixdb-releases/1.2
DATABASE_ROOT=/opt/radixdb/data/databases/app
BACKUP_PARENT=/srv/radixdb-backups
BACKUP="$BACKUP_PARENT/app-$(date -u +%Y%m%dT%H%M%SZ)"

sudo install -d -o radixdb -g radixdb -m 0700 "$BACKUP_PARENT"
sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
sudo -u radixdb "$BUNDLE/backup-external.sh" "$DATABASE_ROOT" "$BACKUP"
sudo systemctl start radixdb
```

Выполните команду для каждой named database, которой нужна recovery point.
Target не должен существовать. Скрипт отклоняет destination внутри source
database, создаёт physical snapshot, получает его точный 32-hex identity,
отклоняет symbolic links, копирует только этот committed snapshot, записывает
inventory и снимает write bits лишь после успешной проверки checksums. При
прерывании incomplete destination удаляется.

## Проверка и хранение backup

Не считайте final success line единственным доказательством. Повторяйте проверку
inventory после переноса и регулярно на хранимом носителе:

```sh
(cd "$BACKUP" && sha256sum -c SHA256SUMS)
find "$BACKUP" -perm /222 -print
```

Вторая команда не должна ничего вывести. Read-only mode защищает от случайной
записи, но не даёт encryption или authentication. Snapshot содержит данные базы
в engine format. Ограничьте доступ к backup location и отдельно защитите копию
`SHA256SUMS`: имеющий возможность заменить и данные, и checksum file сможет
создать новый внутренне согласованный inventory.

Храните release bundle и его `PROVENANCE.env` рядом, но не внутри immutable
backup. Покрытый checksum файл `BACKUP.env` записывает формат external backup,
время создания, CLI version, git revision, build profile и target, digest
Cargo.lock, physical format, database identity и точный snapshot ID. Restore
проверяет все поля до создания target и отклоняет несовместимый physical format.

## Восстановление в новый root

Никогда не проверяйте restore поверх единственной source copy. Release wrapper
требует отсутствующий target, проверяет полный внешний inventory до его создания,
отклоняет symbolic links, копирует snapshots и поручает движку восстановить
проверенную generation. Неудачная работа удаляется, а не становится selectable
database.

Используйте для нового каталога допустимое имя server database: только ASCII
letters, digits, `_` и `-`.

```sh
BUNDLE=/srv/radixdb-releases/1.2
BACKUP=/srv/radixdb-backups/app-20260908T080000Z
RESTORED=/opt/radixdb/data/databases/app-restore-20260908

sudo systemctl stop radixdb
sudo systemctl is-active --quiet radixdb && exit 1
test ! -e "$RESTORED"
sudo -u radixdb "$BUNDLE/restore-external.sh" "$BACKUP" "$RESTORED"
```

Restored root получает новые runtime directories и CONTROL publication только
после проверки manifest, members, checksums, database identity, reachability и
WAL range. Source backup остаётся read-only. Включённые snapshot версии 1.2
index artifacts восстанавливаются; формат, явно исключающий rebuildable index,
всё равно должен пройти engine checks rebuild state.

## Проверка перед переключением

Пока server остановлен, откройте новый root CLI из matching bundle. Проверяйте
application invariants, а не только row count: primary/unique constraints,
запросы по secondary indexes, views, ожидаемые recent rows из WAL и характерные
aggregates.

```sh
sudo -u radixdb "$BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$RESTORED" \
  --execute "SELECT COUNT(*) AS rows, MIN(id) AS first_id, MAX(id) AS last_id FROM items"
sudo -u radixdb "$BUNDLE/bin/radixdb-cli" --quiet --json \
  --db "file://$RESTORED" --execute "SHOW INDEXES FROM items"
```

Закройте CLI, запустите service и выберите восстановленную базу по новому имени.
Дождитесь `ready` от `database_status(name)` и повторите application read. Не
изменяйте старый root, пока restored database не прошла acceptance window.

```sh
sudo systemctl start radixdb
sudo systemctl status radixdb --no-pager
journalctl -u radixdb -n 100 --no-pager
```

Внешний wrapper восстанавливает точный checksum-covered `snapshot_id` из
`BACKUP.env` и не выбирает snapshot по wall-clock order. Он также сверяет, что
скопированный manifest сообщает те же snapshot, database и physical-format
identities, и fail-closed завершается вместо fallback, если выбранный snapshot
отсутствует или повреждён.

## Локальное восстановление

`PRAGMA RESTORE` и CLI `--restore` заменяют текущую database generation. Это
разрушающие rollback tools, а не предпочтительная проверка disaster recovery.
Без ID выбирается последний valid committed snapshot. Конкретный ID состоит
ровно из 32 строчных hexadecimal characters и виден как имя snapshot directory:

```sh
find "$DATABASE_ROOT/snapshots" -mindepth 1 -maxdepth 1 -type d -printf '%f\n'
radixdb-cli --db "file://$DATABASE_ROOT" \
  --restore 0123456789abcdef0123456789abcdef
```

Restore отклоняется внутри explicit transaction. Он прекращает admission новых
transactions, до пяти секунд ждёт активные, готовит и проверяет выбранную
generation, затем выполняет journaled component swap. После успеха process может
продолжить работу, но production rollback всё равно требует исключительного
операционного контроля и проверки после restore.

CLI help использует тот же 32-hex identity format и описывает snapshot retention
на уровне всей базы.

## Physical compatibility и логическая миграция

External wrapper format не обещает, что последующий движок прочитает старую
physical generation. Сначала докажите recovery с matching archived binary. Для
перехода между physical formats экспортируйте старым CLI и импортируйте в
отсутствующий root новым CLI:

```sh
old/radixdb-cli --db file:///srv/radixdb-old --export-sql database.sql
new/radixdb-cli --db file:///srv/radixdb-new --import-sql database.sql
```

Logical export использует один MVCC snapshot и atomic output file. Import
проверяет stream и строит full-sync sibling database до публикации absent target.
Это не заменяет независимые physical backups; полный переход версии определяет
глава upgrading.

## Реакция на ошибки

| Наблюдение | Обязательное действие |
| --- | --- |
| Не удаётся получить source lock | Убедиться, что server и все embedded owners остановлены; не удалять `LOCK` |
| Ошибка snapshot или создания checksums | Не менять source, удалить только incomplete external destination и исправить capacity/I/O |
| `sha256sum -c` не проходит после переноса | Изолировать artifact; не восстанавливать его и не переписывать inventory |
| Restore target уже существует | Выбрать другой отсутствующий root; не удалять базу ради требований wrapper |
| Restore отклоняет member, identity или WAL range | Сохранить logs, backup и matching binary для диагностики; не обходить ошибку копированием отдельных files |
| Restored data отличается от fingerprint recovery point | Не переключаться; сохранить оба root и проверить выбор snapshot и application invariants |

Проверяйте восстановление по расписанию и после каждого release, изменения
storage или backup destination. Записывайте source build identity, имя базы,
backup path, ожидаемый data fingerprint, результат checksum, длительность
restore и identity бинарника, повторно открывшего результат.

См. [архитектуру хранения](../storage/) для generation layout и
[эксплуатацию сервера](../server/) для clean shutdown и database readiness.
