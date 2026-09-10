---
title: Мониторинг
description: Наблюдение за процессом RadixDB, готовностью базы, обслуживанием движка и ресурсами хоста без лишней диагностической нагрузки.
---

Мониторинг должен отвечать на четыре разных вопроса: управляется ли процесс,
принимает ли listener ожидаемый протокол, готова ли конкретная база и не
приближается ли storage engine к пределу ресурсов или обслуживания. Один
зелёный сигнал не подтверждает остальные.

RadixDB 1.2 не предоставляет HTTP health или Prometheus endpoint. Проверенные
интерфейсы: service manager и journal, status methods бинарного протокола, SQL
`PRAGMA RUNTIME_STATS` и счётчики ресурсов операционной системы.

## Процесс и listener

Для установки через systemd начните с unit и первой актуальной ошибки:

```sh
systemctl is-active radixdb
systemctl show radixdb -p MainPID -p ExecMainStatus -p NRestarts
journalctl -u radixdb -n 100 --no-pager
ss -ltn 'sport = :15441'
```

Bundle script `smoke-client.sh` добавляет к проверке listener протокольный
handshake, authentication и build identity. Но он не выбирает прикладную базу.
Нужны alerts на повторные рестарты, неожиданный executable identity, несовпадение
протокола и отсутствие listener на настроенном endpoint.

## Статус сервера и базы

Rust client предоставляет `Connection::server_status()` и
`Connection::database_status(name)`. Результат `ServerStatus` включает build
identity, если capability согласована, lifecycle, ready, список баз и
ограниченные process counters для баз, соединений и in-flight frame bytes.

Глобальный статус используйте для оценки process capacity. Именованный статус
проверяйте после явного выбора прикладной базы: присутствующая на диске, но ещё
не открытая база имеет состояние `Starting` и не готова. При этом global status
может быть `Ready`, поскольку сервер уже принимает sessions.

Перед прикладным трафиком открытая база должна удовлетворять всем условиям:

- named lifecycle равен `Ready`, а `ready` имеет значение true;
- artifact summary имеет `scan_errors = 0` и `truncated = false`;
- negotiated build identity совпадает с одобренной сборкой сервера;
- успешно выполняется короткий характерный read приложения.

`Emergency` означает ошибку open, recovery или close. До рестарта сохраните
message и server journal. Artifact inventory снимается при открытии базы и
кэшируется в lifecycle registry; обычный status polling не обходит filesystem.
Каждый scan ограничен 4096 entries и depth 8, содержит process-local `sequence`
и `sampled_unix_millis` и пропускает retained snapshot trees. Поэтому
`complete = false` может означать `snapshots_omitted = true`; поля `truncated` и
`scan_errors` отделяют исчерпание бюджета от I/O failure.

## Статистика движка

`PRAGMA RUNTIME_STATS` возвращает одно значение TEXT с версионированным JSON.
Выполняйте его через выбранное application connection. Локальный CLI допустим,
только когда он является единственным владельцем file database; нельзя
параллельно открывать root, принадлежащий серверу.

```sql
PRAGMA RUNTIME_STATS;
```

Текущий payload имеет `format = 2`. Сбор использует atomics и try-locks, не
открывает payload files и ограничивает число посещаемых структур. Вместе с
sample всегда сохраняйте `format`, `sequence`, `captured_unix_millis` и
`snapshot_nanos`.

Сначала проверяйте `complete`, `missing_evidence` и `truncated_owners`. При
`complete = false` затронутые totals являются нижними границами. Нулевое
значение при missing или truncated evidence нельзя считать признаком здоровья.

| Область | Основные поля | Эксплуатационное значение |
| --- | --- | --- |
| Transactions | `active_transactions`, `oldest_transaction_age_millis`, `transaction_wait_edges` | Рост возраста или waits может удерживать visibility и задерживать обслуживание |
| Hot и staging | `hot_rows`, `hot_bytes`, `staging_transactions`, `staging_rows` | Устойчивый рост указывает на задержку seal, длинную транзакцию или нагрузку выше возможностей maintenance |
| Cold levels | `cold_unleveled_segments`, `cold_l0_segments`, `cold_l0_debt_physical_bytes` | Сравнивайте trend с soft/hard limits; ненулевой L0 сам по себе не ошибка |
| Compaction | requested/running, active jobs, retry cooldown, backpressure counters | Повторный cooldown или рост hard rejections требует проверки capacity и I/O |
| WAL | `wal_running`, file/max bytes, pending durability, checkpoint time | У готовой persistent database WAL должен работать; следите за застывшим ростом и ошибками checkpoint |
| Maintenance | worker alive, calls, failures и timestamps seal/compaction/checkpoint | Следите за приростом failure counters и отсутствием успешного progress при ожидающей работе |
| Page cache | состояние `page_cache_warmup`, target, warmed и resident estimates | Сравнивайте с уровнем, safe budget и памятью хоста; disabled нормально при такой настройке |
| Storage CPU | configured/effective/in-use/peak workers и leases | Длительное насыщение помогает обосновать tuning, но само по себе не означает corruption |

## Ресурсы хоста

Собирайте filesystem bytes и inodes для фактического mount базы, а не только
для `/`. Записывайте RSS процесса, доступную память, swap activity и ошибки
kernel/storage. `du` является диагностическим sample и на большом дереве само
может создавать metadata I/O.

```sh
DATA=/opt/radixdb/data
df -B1 "$DATA"
df -i "$DATA"
du -sx --block-size=1 "$DATA/databases"
ps -o pid,etimes,rss,vsz,%cpu,stat,cmd -C radixdb-server
grep -E 'MemAvailable|SwapFree|Dirty|Writeback' /proc/meminfo
journalctl -k -p warning..alert --since '15 minutes ago' --no-pager
```

Следите за trend свободных bytes и inodes заранее, оставляя место для WAL,
checkpoint, compaction output и внутреннего snapshot. Compaction резервирует
место сверх планируемого output; cooldown `disk_reserve_exhausted` является
ранней защитой, но не доказывает, что filesystem уже заполнена.

## Политика оповещений

Hard limits движка используйте как жёсткие пороги, а рабочие rates определяйте
по реальной нагрузке. Полезные alerts:

- named database не готова или lifecycle равен `Emergency`;
- artifact scan неполон или содержит scan error;
- использование connections или in-flight frames приближается к настроенному максимуму;
- runtime sample неполон, усечён или необычно долго собирается;
- WAL не работает, растёт durability backlog или число ошибок checkpoint;
- maintenance worker отсутствует при ready lifecycle;
- L0 debt растёт к hard limit, повторяется retry cooldown или появляются новые
  hard backpressure rejections;
- одновременно растут возраст старых transactions и wait edges;
- заканчиваются filesystem bytes/inodes, возникают OOM events или kernel I/O errors.

Для cumulative counters отслеживайте приращения, а trend-alert подтверждайте
несколькими samples. Оставьте один редкий end-to-end probe, который выбирает
базу и выполняет read: process-only проверки не обнаружат ошибку открытия базы.

Процедуры реакции приведены в главе [диагностика](../troubleshooting/), а RSS и
page cache подробнее разобраны в [управлении памятью](../memory/).
