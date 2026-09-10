---
title: Конфигурация сервера
description: Выбор, проверка и применение server.toml и allowlist native plugins.
---

`radixdb-server` читает один TOML-документ с обязательной верхнеуровневой
таблицей `[server]` и необязательной верхнеуровневой таблицей `[plugins]`.
Конфигурация управляет listener, лимитами процесса, allowlist native packages и
storage settings, которые передаются каждой базе при открытии. Неизвестные
таблицы и ключи являются ошибкой; опечатка в имени лимита не игнорируется.

## Выбор файла

Сервер использует два правила выбора файла:

1. `--config PATH` читает ровно `PATH`.
2. Без `--config` читается `server.toml` из рабочего каталога процесса.

У сервера нет environment variable или второго config file, переопределяющего
отдельные TOML values. Переменные с префиксом `RADIXDB_RELEASE_` принадлежат
release lifecycle scripts, а не parser конфигурации сервера. Systemd unit всегда
передаёт абсолютный путь установленного config.

```sh
radixdb-server --config /etc/radixdb/server.toml
radixdb-server --config /etc/radixdb/server.toml --print-endpoint
```

`--print-endpoint` проверяет TOML decoding и выводит `bind_ip port`; он не
выполняет полную runtime validation, не допускает plugin packages, не проверяет
filesystem permissions и не резервирует port. Окончательной проверкой остаётся
реальный foreground start или restart службы.

## Release template

В release bundle 1.2 входит следующий полный template. Все размеры задаются
целым числом байтов; суффиксы `MiB` и `GiB` не принимаются.

```toml
[server]
bind_ip = "127.0.0.1"
port = 15441
data_dir = "data"
max_connections = 64
max_inflight_frame_bytes = 268435456
max_databases = 64
max_database_name_bytes = 64
connect_timeout_secs = 10
connection_idle_timeout_secs = 28800
net_read_timeout_secs = 30
net_write_timeout_secs = 60
cursor_batch_max_rows = 1024
cursor_batch_max_bytes = 8388608
max_frame_bytes = 67108864
copy_max_transaction_bytes = 536870912
max_compaction_jobs = 1
storage_cpu_workers = 0
page_cache_level = 0
page_cache_max_bytes = 0
page_cache_memory_reserve = 0
target_volume_rows = 1048576
seal_hot_bytes_threshold = 67108864
seal_incremental_hot_bytes_threshold = 16777216
read_queue_depth = 1

# [server.authentication]
# root_password_verifier = "$argon2id$..."

# [plugins]
# package_directories = ["/opt/radixdb/plugins/radix-spatial-1.0.0"]
```

По умолчанию используется обычный TCP с логином/паролем, сертификат для него не
нужен. Для direct TLS добавьте необязательную вложенную таблицу транспорта:

```toml
[server.transport]
mode = "tls"
certificate_chain = "/etc/radixdb/tls/server-chain.pem"
private_key = "/etc/radixdb/tls/server-key.pem"
```

В TLS-режиме обязательны оба файла, а private key не должен читаться group или
other users. Для обычного TCP endpoint с password authentication не указывайте
`[server.transport]` либо выберите `mode = "plaintext"`.

Чтобы требовать пароль для административного входа `root`, добавьте
необязательную таблицу authentication с полным результатом
`radixdb-password`:

```toml
[server.authentication]
root_password_verifier = "$argon2id$..."
```

Сервер принимает только корректный Argon2id PHC verifier в поддерживаемых
границах безопасности. Значение скрывается в debug output, но остаётся
учётными данными и должно быть защищено правами файловой системы. При наличии
настройки беспарольный `root` отключён на каждом endpoint. При её отсутствии
беспарольное восстановление доступно только через plaintext loopback. Создание
verifier, вход и ротация описаны в главе
[«Аутентификация»](../authentication/).

Native extensions используют явный startup allowlist:

```toml
[plugins]
package_directories = [
  "/opt/radixdb/plugins/radix-spatial-1.0.0",
]
```

Каждое значение называет один точный package directory, а не родительский
каталог для сканирования. Path должен быть absolute, normalized и не содержать
symbolic links. При отсутствии или пустой таблице server не ищет plugin files и
создает registry generation zero. Server допускает все перечисленные packages
до bind listener; одна неверная entry останавливает весь startup. Ownership,
package format и замена version описаны в главе
[«Extensions»](../extensions/).

В стандартной установке задан `WorkingDirectory=/opt/radixdb`, поэтому
относительный data path становится `/opt/radixdb/data`. Если рабочий каталог не
контролируется service definition, используйте абсолютный путь.

## Справочник параметров

Code default применяется только при отсутствии необязательного ключа. Release
value явно записан в упакованном `server.toml` и потому имеет приоритет. У трёх
обязательных ключей code default отсутствует.

| Ключ | Code default | Release value | Допустимые значения | Область применения |
| --- | ---: | ---: | --- | --- |
| `bind_ip` | required | `127.0.0.1` | valid IP address | Listener и authentication boundary |
| `port` | required | `15441` | `1..=65535` | Listener |
| `data_dir` | required | `data` | non-empty UTF-8 path without `?` | Корень баз |
| `transport` | `plaintext` | omitted (`plaintext`) | `plaintext` or TLS table | Политика шифрования socket |
| `authentication.root_password_verifier` | absent | absent | Argon2id PHC string | Необязательный verifier пароля `root` |
| `plugins.package_directories` | empty | omitted | exact absolute package paths | Startup registry native plugins |
| `max_connections` | `151` | `64` | `> 0` | Process admission |
| `max_inflight_frame_bytes` | `268435456` | `268435456` | `> 0`, at least `max_frame_bytes` | Process frame budget |
| `max_databases` | `64` | `64` | `> 0` | Process database registry |
| `max_database_name_bytes` | `64` | `64` | `> 0` | Выбор базы |
| `connect_timeout_secs` | `10` | `10` | `> 0` seconds | Initial handshake и authentication |
| `connection_idle_timeout_secs` | `28800` | `28800` | `> 0` seconds | Idle session |
| `net_read_timeout_secs` | `30` | `30` | `> 0` seconds | Чтение frame payload |
| `net_write_timeout_secs` | `60` | `60` | `> 0` seconds | Socket write |
| `cursor_batch_max_rows` | `1024` | `1024` | `> 0` rows | Legacy row batch |
| `cursor_batch_max_bytes` | `8388608` | `8388608` | `> 0`, at most `max_frame_bytes` | Cursor batch |
| `max_frame_bytes` | `67108864` | `67108864` | `256..=max_inflight_frame_bytes` bytes | Binary protocol frame |
| `copy_max_transaction_bytes` | `536870912` | `536870912` | `> 0` bytes | Один атомарный `COPY FROM` |
| `max_compaction_jobs` | `1` | `1` | `1..=8` | Concurrent table-local compactions |
| `storage_cpu_workers` | `0` | `0` | `>= 0` workers | Общий seal/compaction CPU pool |
| `page_cache_level` | `0` | `0` | `0..=10` | Прогрев OS page cache |
| `page_cache_max_bytes` | `0` | `0` | `>= 0` bytes | Предел прогрева |
| `page_cache_memory_reserve` | `0` | `0` | `>= 0` bytes | Память вне прогрева |
| `target_volume_rows` | `1048576` | `1048576` | `>= 65536` rows | Форма новых cold volumes |
| `seal_hot_bytes_threshold` | `67108864` | `67108864` | `> 0` bytes | Первый hot-buffer seal |
| `seal_incremental_hot_bytes_threshold` | `16777216` | `16777216` | `> 0` bytes | Последующие incremental seals |
| `read_queue_depth` | `1` | `1` | `> 0` requests | Sequential cold reads |

## Лимиты listener и протокола

Release выбирает 64 одновременных соединения, хотя при отсутствии ключа code
default равен 151. Увеличение лимита также увеличивает число sessions,
конкурирующих за frame, cursor и storage resources; это не изолированный
переключатель throughput.

`max_inflight_frame_bytes` является общим process admission budget для
одновременных frame payloads. Каждый `max_frame_bytes` должен помещаться в него,
а каждый cursor batch в один frame. Нижняя граница 256 байт сохраняет возможность
представить control messages протокола. Это границы безопасности памяти и
протокола, а не лимит результата запроса: клиент может получить cursor несколькими
batches.

Четыре timeout относятся к разным фазам. `connect_timeout_secs` охватывает
initial setup и authentication. После подключения idle session может не
присылать новый frame в течение `connection_idle_timeout_secs`; для частичного
frame действует `net_read_timeout_secs`, для записи `net_write_timeout_secs`.

## Параметры хранения и памяти

Storage settings передаются базе при открытии сервером. Они не внедряются
задним числом в уже открытый engine.

`copy_max_transaction_bytes` является консервативным бюджетом одного атомарного
`COPY`: он учитывает оценку усиления MVCC и WAL и не является process RSS limit.
`target_volume_rows` влияет на форму новых immutable volumes, поэтому меняйте
его как storage-layout решение до большой загрузки.

`storage_cpu_workers = 0` использует CPU, видимые процессу или cgroup.
Положительное значение ограничивает общий seal/compaction pool.
`max_compaction_jobs` задаёт concurrent jobs для разных таблиц и никогда не
разрешает двух владельцев одной таблицы. Сохраняйте переносимый default 1, пока
не измерены целевой диск и workload.

Прогрев page cache выключен на level 0. Level 10 запрашивает полную текущую
generation только в пределах эффективного cap и memory reserve; прогрев остаётся
оптимизацией и не требуется для корректности. `page_cache_max_bytes = 0` и
`page_cache_memory_reserve = 0` выбирают автоматические policies. Запросы, MVCC
state, metadata и page cache ОС остаются вне любого отдельного budget.

`read_queue_depth = 1` является переносимым release setting. Большее значение
может помочь измеренным sequential reads на подходящем storage, но random
primary-key paths остаются на depth 1, и ни одно значение не является
универсальным ускорителем.

## Применение изменений

Сервер читает конфигурацию только при запуске процесса. Изменение файла не
перезагружает listener или storage settings. Используйте следующую процедуру:

1. Сохраните рабочую конфигурацию.
2. Измените одну связанную группу значений и проверьте TOML decoding через
   `--print-endpoint`.
3. Перезапустите сервер, затем проверьте journal на ошибки полной validation и
   открытия баз.
4. Запустите protocol smoke и дождитесь ready для каждой application database.
5. При ошибке validation или workload верните прежний файл и снова выполните
   restart.

```sh
sudo systemctl restart radixdb
sudo systemctl status radixdb --no-pager
cd /opt/radixdb && ./smoke-client.sh
```

Изменение listener values разрывает соединения при restart. Изменение storage
или plugin values тоже требует закрытия и повторного открытия баз, которое
выполняет clean shutdown сервера. Не редактируйте database artifacts для
применения настройки.

В главе [«Установка сервера»](../installation/) заданы ownership и замена config
file. [«Работа сервера»](../server/) объясняет readiness и clean restart. Главы
о storage и memory дадут рекомендации для конкретных workloads; до их публикации
сохраняйте release values, если изменение не обосновано измеренным тестом.
