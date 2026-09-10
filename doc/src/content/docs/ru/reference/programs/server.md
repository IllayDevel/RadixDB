---
title: radixdb-server
description: Справочник command line protocol server RadixDB.
---

`radixdb-server` владеет файловыми базами в одном корне данных и принимает
клиентов протокола 17. Набор команд намеренно невелик.

## Синопсис

```text
radixdb-server [--config server.toml] [--print-endpoint] | --help | --version
```

| Invocation | Result |
| --- | --- |
| `radixdb-server` | Прочитать `server.toml` из process working directory и запуститься |
| `radixdb-server --config PATH` | Прочитать ровно `PATH` и запуститься |
| `radixdb-server --print-endpoint` | Разобрать default config и вывести `bind_ip port` |
| `radixdb-server --config PATH --print-endpoint` | Разобрать выбранный config и вывести `bind_ip port` |
| `radixdb-server --version` | Вывести build и protocol identity без чтения config |
| `radixdb-server --help`, `-h` | Вывести usage и успешно завершиться без чтения config и запуска server |

Две options print-endpoint могут идти в любом порядке. Unknown или conflicting
arguments возвращают usage line и ненулевой status.

## Выбор конфигурации

Command-line overrides отдельных server settings и environment-variable layer
отсутствуют. TOML file должен содержать top-level table `[server]` и может
содержать top-level allowlist packages `[plugins]`. Unknown keys отклоняются.

```sh
radixdb-server --config /etc/radixdb/server.toml --print-endpoint
radixdb-server --config /etc/radixdb/server.toml
```

`--print-endpoint` доказывает только TOML decoding. Команда не выполняет полную
runtime validation, не допускает plugin packages, не создаёт `data_dir`, не
резервирует port и не открывает database. Start готов принимать sessions только
после log line `listening on`.

## Поведение process

Без explicit config path относительные пути разрешаются от текущего working
directory. Внутри config относительный `data_dir` также основан на process
working directory. Packaged systemd unit контролирует оба значения.

`SIGTERM` и `SIGINT` запрашивают clean shutdown. Успешное завершение закрывает
sessions и все открытые databases и пишет `radixdb-server stopped cleanly`.
Startup или runtime failure возвращает ненулевой process status и добавляет к
stderr prefix `radixdb-server:`.

До bind listener server атомарно допускает каждый package из `[plugins]` и
выводит bounded registry summary с counts packages, types, functions,
operators, operator classes, planner support, shadowed versions и library
bytes. Пустой allowlist сообщает registry generation zero. Любая ошибка package
validation останавливает startup; частичный registry не становится видимым.
См. главу [«Extensions»](../../../administration/extensions/).

Server поддерживает database-bound login/password authentication через обычный
TCP и опциональный direct TLS. Настроенный verifier root требует исходный
пароль на каждом endpoint; без verifier беспарольный root разрешён только как
loopback plaintext recovery endpoint. Перед публикацией transport прочитайте
[«Аутентификация»](../../../administration/authentication/).
Полный набор параметров находится в
[«Конфигурации сервера»](../../../administration/configuration/).

## radixdb-password

`radixdb-password` создаёт Argon2id PHC verifier, который принимает
`server.authentication.root_password_verifier`.

```text
radixdb-password [--help | --version]
```

Без option программа читает одну строку пароля из перенаправленного стандартного
ввода и выводит одну строку verifier. Пароль должен быть корректным UTF-8 длиной
от 1 до 1024 байт. Ввод из интерактивного терминала отклоняется, поскольку
утилита сама не отключает echo; передать пароль аргументом командной строки
нельзя. `--help` и `-h` выводят usage, а `--version` показывает те же поля build
identity, что остальные release-программы.

Пустой, многострочный, слишком длинный или не-UTF-8 ввод завершается ошибкой без
verifier. Защищайте результат как учётные данные и сохраняйте строку полностью,
включая каждый разделитель `$`. Рабочая процедура приведена в главе
[«Аутентификация»](../../../administration/authentication/).
