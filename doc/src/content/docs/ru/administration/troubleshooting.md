---
title: Диагностика неисправностей
description: Диагностика ошибок запуска, заполнения диска, I/O и восстановления RadixDB с сохранением доказательств и вариантов recovery.
---

Диагностика начинается с сохранения первой ошибки и остановки дальнейших
изменений состояния. Restart может восстановить процесс после обычного падения,
но повторные автоматические рестарты способны вытеснить полезные логи и снова
нагружать неисправное хранилище.

Нельзя вручную удалять или заменять `LOCK`, CONTROL, WAL, catalogs, manifests,
DATA, INDEX, snapshots, staging или quarantine files. Они участвуют в ownership,
reachability и atomic publication. Удаление одного artifact может превратить
диагностируемую ошибку в постоянную потерю.

## Первые действия

Остановите прикладной трафик. Если сервис перезапускается или сообщает об I/O
либо corruption, остановите unit до сбора данных:

```sh
sudo systemctl stop radixdb
systemctl status radixdb --no-pager
journalctl -u radixdb --since '30 minutes ago' --no-pager
radixdb-server --version
sha256sum /opt/radixdb/bin/radixdb-server /opt/radixdb/bin/radixdb-cli
```

Запишите точное время, последнюю подтверждённую операцию приложения, endpoint,
имя базы, build identity, checksum конфигурации, filesystem mount и первую
ошибку. Если device читается, до repair сохраните внешнюю побайтовую копию или
storage snapshot. Не записывайте эту копию в пострадавшую filesystem базы.

## Классификация ошибки

| Наблюдение | Вероятная граница | Следующее действие |
| --- | --- | --- |
| Unit неактивен до появления listener | Конфигурация, permissions, bind или process failure | Прочитать первую ошибку journal; проверить пути и endpoint |
| Listener работает, named database имеет `Starting` | База ещё не была выбрана | Выбрать её и дождаться named readiness |
| Named database имеет `Opening` или recovering | Идёт строгий open и WAL replay | Ждать с ограниченным deadline, наблюдать journal и host I/O |
| Named database имеет `Emergency` | Ошибка open/recovery/close | Остановить restart loop, сохранить message и storage copy |
| `disk_reserve_exhausted` | Compaction не может сохранить настроенный резерв свободного места | Добавить capacity или переместить посторонние данные за пределы database tree |
| `ENOSPC` или закончились inodes | Filesystem больше не принимает необходимые записи | Остановить writes, освободить место вне database tree, затем проверить |
| `failed to write to WAL` или `failed to sync WAL` | Ошибка write path, device, mount или filesystem I/O | При потере соединения считать исход commit неопределённым; проверить OS errors |
| Отсутствует или повреждён обязательный DATA/catalog/manifest | Потеря media или artifact, а не обычный crash | Восстановить проверенный external backup в новый root |
| Optional index недоступен | Rebuildable accelerator не прошёл validation | Сохранить диагностику; использовать только документированный scan fallback и rebuild path |

## Заполнение диска

Проверяйте и bytes, и inodes на mount базы. Найдите источник роста, не удаляя
файлы движка:

```sh
DATA=/opt/radixdb/data
df -B1 "$DATA"
df -i "$DATA"
du -x --max-depth=2 --block-size=1 "$DATA" | sort -n
journalctl -u radixdb -g 'ENOSPC|disk_reserve_exhausted|backpressure|checkpoint|WAL' --no-pager
```

Compaction нужно место для нового immutable output, пока старые inputs остаются
reachable. Поэтому движок резервирует свободное место сверх output budget. При
ошибке reserve check конкретный compaction input входит в retry cooldown. Writes
могут продолжаться до hard backpressure L0; не следует ждать этого отказа перед
добавлением capacity.

Настоящий `ENOSPC` отличается от исчерпания резерва. WAL append, sync, checkpoint
или snapshot может вернуть ошибку. Проверенный ENOSPC failpoint отклоняет
затронутый insert, не публикует его строку, а база повторно открывается. Этот
тест не обещает такой же области отказа для любого реального device failure.
Остановите новые writes, освободите или расширьте место вне database root,
проверьте device, затем откройте тем же бинарником и проверьте инварианты.

## Ошибки I/O и durability

До анализа SQL проверьте mount и kernel:

```sh
findmnt -T /opt/radixdb/data -o TARGET,SOURCE,FSTYPE,OPTIONS
journalctl -k -p warning..alert --since '30 minutes ago' --no-pager
cat /proc/mounts
```

Write error, дошедшая до клиента, означает неуспешную операцию. Если соединение
потеряно до ответа commit, клиент может не знать, стал ли commit durable. Нельзя
слепо повторять неидемпотентную запись: сначала подключитесь, найдите операцию по
application idempotency key или transaction identity и выполните reconciliation.

`sync_mode=normal` синхронизирует commit и DDL durability boundaries;
`sync_mode=full` синхронизирует каждую WAL write. `sync_mode=none` не форсирует
WAL sync, поэтому подтверждённые операции до durable checkpoint имеют более
слабую гарантию при потере питания. Ни один режим не защищает от потери самого
storage device. Развёрнутое значение приведено в [конфигурации](../configuration/).

После I/O error нельзя возобновлять работу только потому, что одна запись прошла.
Проверьте filesystem, controller и kernel log, затем выполните controlled restart
и прикладной read/write/rollback probe. Повторные sync failures требуют вывести
базу из эксплуатации и восстановить или перенести её.

## Crash и потеря носителя

| Событие | Что может восстановить RadixDB | Что остаётся вне гарантии |
| --- | --- | --- |
| Clean stop | Закрытое состояние движка и durable checkpoint/WAL | Потеря hardware после остановки всё равно требует backup |
| Process crash, `SIGKILL` или host reset при целой durable storage | Выбор нового полного CONTROL generation и replay непрерывного committed WAL suffix | Acknowledgements слабее настроенного sync mode; внешние эффекты приложения |
| Power loss | То же строгое recovery из bytes, которые storage фактически сделало durable | Volatile device caches и acknowledgements `sync_mode=none` |
| Отсутствует или повреждён обязательный artifact | Можно выбрать предыдущее полное CONTROL, если весь его reachable graph и WAL исправны | WAL не заменяет отсутствующий committed DATA/catalog/manifest |
| Потеря filesystem или device | Ничего с этого device | Нужен независимый external backup на исправном storage |

Обычное crash recovery является открытием существующего root подходящим
бинарником. Оно проверяет оба CONTROL slots, выбирает только полное reachable
generation и по порядку воспроизводит committed WAL. Повреждённые или неполные
records не публикуют частичные transactions. Отсутствующий optional index может
быть помечен unavailable; отсутствие required DATA делает candidate неверным.

Media recovery устроено иначе. Восстановите проверенный по checksum external
backup в отсутствующий root на исправном storage. Нельзя создавать замену root,
копируя отдельные уцелевшие файлы в обход validation error. Если полного CONTROL
candidate нет, fail-closed open является правильным результатом.

## Контролируемое восстановление

1. Оставьте failed root и evidence неизменными.
2. Проверьте storage и capacity хоста; устраните проблему платформы до нового
   обращения к базе.
3. Точным source bundle выполните одну controlled open копии или storage
   snapshot. Сохраните полную ошибку.
4. Если strict recovery успешно, до возврата трафика проверьте rows, constraints,
   indexes, views и последние committed факты приложения.
5. При потере required artifacts или ошибке strict open восстановите проверенный
   external backup в новый отсутствующий root и проверьте его до cutover.
6. Для несовместимой target release сначала восстановите исходным бинарником,
   затем выполните логическую [процедуру обновления](../upgrading/).

`--reset-storage` и локальный `PRAGMA RESTORE` являются destructive recovery
tools, а не общими исправлениями corruption. Они могут заменить или поместить в
quarantine текущее generation и требуют exclusive ownership. Используйте их
только по отдельному runbook, имея сохранённую копию и известный recovery point.

## Возврат в работу

Запустите сервер один раз, проверьте build identity, выберите базу, потребуйте
named ready status с полным artifact scan и выполните прикладные проверки.
Сравните результат с записанным recovery point. Сохраните предыдущий root и
логи на всё окно наблюдения.

RadixDB 1.2 не заявляет high availability, replication, automatic failover,
point-in-time recovery к произвольной позиции WAL или recovery после потери
media без external backup. Эксплуатационная процедура не должна превращать
строгий отказ в молчаливый fallback.

Disaster recovery описано в [резервном копировании](../backup-restore/), а
диагностические сигналы - в главе [мониторинг](../monitoring/).
