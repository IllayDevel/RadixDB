---
title: Работа сервера
description: Запуск, проверка и остановка radixdb-server с соблюдением владения файлами и границ готовности.
---

`radixdb-server` владеет файловыми базами и предоставляет бинарный протокол
RadixDB по TCP. Клиент подключается, выполняет handshake и authentication, затем
выбирает именованную базу перед выполнением SQL. Сервер открывает базы по запросу
и хранит их в registry процесса до остановки.

Не открывайте принадлежащую серверу базу одновременно из embedded-кода или
`radixdb-cli`. CLI является локальным embedded/file-инструментом, а не TCP-shell.

## Минимальная конфигурация

Файл конфигурации содержит обязательную верхнеуровневую таблицу `[server]` и
может содержать необязательную `[plugins]`. Неизвестные поля отклоняются.
Минимальная практическая конфигурация:

```toml
[server]
bind_ip = "127.0.0.1"
port = 15441
data_dir = "data"
```

Относительный `data_dir` разрешается от рабочего каталога процесса, а не от
каталога с `server.toml`. Упакованный systemd unit задаёт рабочим каталогом
prefix установки, поэтому его путь `data` стабилен. Все settings, plugin
allowlist, defaults и правила validation приведены в главе
[«Конфигурация сервера»](../configuration/).

В минимальной конфигурации нет verifier пароля root, поэтому `root` без пароля
разрешён только на этом plaintext loopback endpoint. Для штатного
администрирования создайте Argon2id verifier утилитой `radixdb-password`,
добавьте его как `server.authentication.root_password_verifier` и
перезапустите сервер. После этого исходный пароль обязателен на каждом endpoint,
а намеренный non-loopback bind разрешён; используйте direct TLS, если весь
сетевой путь не является доверенным. Приложения должны входить как субъекты
базы, а не как `root`.

Посмотреть endpoint без открытия listener можно так:

```sh
radixdb-server --config server.toml --print-endpoint
```

Вывод содержит host и port, разделённые пробелом. `--print-endpoint` разбирает
файл, но не допускает настроенные plugin packages и не доказывает, что порт
свободен, data directory доступен для записи и сервер способен запуститься.

## Прямой запуск

Для foreground-проверки выполните:

```sh
radixdb-server --config server.toml
```

После валидации конфигурации process допускает весь plugin allowlist до bind и
сначала выводит registry summary:

```text
radixdb-server plugin registry generation=0 packages=0 types=0 functions=0 operators=0 operator_classes=0 planner_support=0 shadowed_versions=0 library_bytes=0
```

Неверный package атомарно останавливает startup. После создания data directory
и успешного bind process выводит:

```text
radixdb-server listening on 127.0.0.1:15441
```

Эта строка подтверждает только готовность listener. Она не означает, что
конкретная база уже завершила открытие или восстановление.

Без аргументов программа читает `server.toml` из рабочего каталога. Параметр
`--version` выводит идентичность без чтения конфигурации и открытия listener.
Не объединяйте `--version` с другими параметрами.

## Выбор базы и файлы

В wire protocol нет отдельной команды `CREATE DATABASE`. После handshake и
authentication вызов `select_database(name)` открывает существующую базу или
создаёт:

```text
<data_dir>/databases/<name>/
```

Имя содержит от 1 до `max_database_name_bytes` байт и только ASCII letters,
digits, `_` или `-`. Упакованный default равен 64 байтам. Соединение не может
сменить базу при активной явной транзакции или открытом cursor.

Открытие выполняется лениво. Logs различают `state=opening`, `state=ready` и
`state=failed`. Неудачное открытие остаётся доступным для диагностики и получает
ограниченную возможность повтора после устранения причины. Параметр
`max_databases` ограничивает записи process registry, включая diagnostic states.

## Проверка готовности

Для установленного bundle сначала запустите готовый protocol probe:

```sh
cd /opt/radixdb
./smoke-client.sh
```

Успешный результат имеет следующий вид:

```text
ready version=1.1.0 protocol=17 state=Ready
```

Он подтверждает listener, handshake, authentication, lifecycle сервера и
согласованную build identity. Application database при этом не выбирается.

Приложение должно вызвать `database_status(name)`, дождаться `ready = true`
после выбора или восстановления базы, затем выполнить дешёвое прикладное чтение.
`server_status()` описывает lifecycle процесса; `database_status()` дополнительно
сообщает состояние базы и счётчики artifacts. TCP connect и строка listener в
логе необходимы, но недостаточны как признаки готовности.

## Работа с systemd

Установленный unit запускает сервер как `radixdb:radixdb`, задаёт
`WorkingDirectory=/opt/radixdb` и ограничивает запись каталогами data, logs и
run. Обычные команды:

```sh
sudo systemctl status radixdb --no-pager
sudo systemctl restart radixdb
journalctl -u radixdb -n 100 --no-pager
```

Unit использует `Restart=on-failure`, задержку повторного запуска две секунды и
60 секунд на остановку. Штатная остановка оператором не считается сбоем.
Авторитетен файл `radixdb.service` из проверенного bundle; после нестандартного
prefix проверьте сгенерированные пути.

## Штатная остановка и повторный запуск

Сигналы `SIGTERM` и `SIGINT` запрашивают штатную остановку. Сервер отменяет
активные запросы, закрывает session sockets, ожидает workers и затем закрывает
все открытые базы. Успешная foreground-остановка завершается строкой:

```text
radixdb-server stopped cleanly
```

Для установленного процесса используйте service manager:

```sh
sudo systemctl stop radixdb
sudo systemctl start radixdb
```

Lifecycle wrappers из release bundle записывают PID и время запуска Linux
process, проверяют исполняемый файл и listener, сериализуют переходы через
`flock` и отказываются сигналить stale или foreign PID. Они полезны в bundle
sandbox; после установки владельцем процесса остаётся systemd.

Проверенный restart test подтверждает, что committed data переживает повторное
открытие, а rolled-back row остаётся отсутствующей. Это функциональная проверка
рестарта, но не backup. Для важных данных сохраняйте независимые резервные копии
и проверяйте restore procedures.

## Ошибки запуска

Перед изменением конфигурации найдите первую ошибку в journal:

- отсутствующий путь config завершается ошибкой до bind;
- неизвестный ключ или недопустимое значение отклоняется;
- относительный data path может разрешиться не туда при неверном working directory;
- недоступный для записи data directory не позволяет запустить сервер;
- занятый port не позволяет выполнить bind;
- TLS не запускается с отсутствующим или неверным сертификатом либо с
  небезопасными правами private key.
- plugin startup завершается ошибкой при неверном path, manifest, checksum,
  platform, descriptor, dependency или filesystem ownership package.

Привязка к non-loopback адресу разрешена. Защитите её прямым TLS или доверенной
сетью и аутентифицируйте приложения как субъекты базы. Административный `root`
там требует настроенный verifier пароля; без него доступен только
восстановительный путь через plaintext loopback.

После исправления причины снова запустите службу и повторите protocol probe и
проверку готовности application database. Layout bundle и файлов описан в
главе [«Установка сервера»](../installation/), а изолированный foreground-пример
находится в [учебнике сервера](../../tutorial/server-connection/).

Placement native packages, exact-version bindings и восстановление при
отсутствующем package описаны в главе [«Extensions»](../extensions/).
