---
title: Установка сервера
description: Сборка проверяемого release bundle и установка TCP-сервера RadixDB как службы Linux systemd.
---

RadixDB может работать внутри приложения или отдельным процессом
`radixdb-server`. Эта глава устанавливает TCP-сервер из checkout исходников.
Поддерживаемый layout службы представляет собой проверенный release bundle в
`/opt/radixdb`; копирование одного исполняемого файла без конфигурации, lifecycle
scripts и provenance record не является той же установкой.

## Проверенная платформа

Процедура 1.2 выполнена на Fedora Linux 44, x86-64, с GNU ABI и Rust 1.97.0 из
`rust-toolchain.toml`. Полученные бинарники имеют target
`x86_64-unknown-linux-gnu` и динамически используют glibc, `libm` и `libgcc_s`.

Исходники написаны на переносимом Rust, но это не делает каждую ОС и архитектуру
проверенной серверной платформой. Для другого target нужны собственные проверки
сборки, файловой системы, сигналов, восстановления и service manager. Готовые
installer и unit предназначены именно для Linux с systemd.

Установите следующие зависимости сборки и упаковки:

| Назначение | Необходимый инструмент |
| --- | --- |
| Компиляция | Rust 1.97.0 с Cargo, C compiler и linker |
| Разрешение зависимостей | Доступ к Cargo registry или заполненный cache |
| Отделение и проверка символов | GNU `objcopy` и `readelf` |
| Проверка bundle | `sha256sum` |
| Установка службы | `install`, `sed`, `getent`, `groupadd`, `useradd` и systemd |

## Сборка release-программ

Запускайте сборку на выбранной для развёртывания ревизии исходников. Параметр
`--locked` не позволяет Cargo незаметно изменить разрешение зависимостей.

```sh
cargo build --locked --release \
  --bin radixdb-server \
  --bin radixdb-password \
  --bin radixdb-cli \
  --bin radixdb-smoke-client \
  --features cli
```

До упаковки проверьте идентичность всех четырёх программ:

```sh
./target/release/radixdb-server --version
./target/release/radixdb-password --version
./target/release/radixdb-cli --version
./target/release/radixdb-smoke-client --version
```

Git revision, profile, target и digest Cargo lock должны совпадать. База
исходников этого руководства 1.2 сейчас сообщает версию package `1.1.0`;
точный проверяемый artifact определяется полной ревизией и версией протокола.

```text
radixdb-server 1.1.0 git=<40-hex-revision> protocol=17 profile=release target=x86_64-unknown-linux-gnu lock=<64-hex-sha256>
```

Не развёртывайте идентичность с окончанием `-dirty`, если незакоммиченное
состояние исходников не является намеренным и не сохранено отдельно.

## Создание и проверка bundle

Выходной каталог не должен существовать. Packager устанавливает компактные
исполняемые файлы, сохраняет отдельные debug files с совпадающими ELF Build IDs,
создаёт `PROVENANCE.env` и покрывает каждый файл списком `SHA256SUMS`.

```sh
test ! -e dist
release/package-artifacts.sh dist \
  target/release/radixdb-server \
  target/release/radixdb-cli \
  target/release/radixdb-smoke-client
(cd dist && sha256sum -c SHA256SUMS)
```

Сохраняйте весь результат вместе:

```text
dist/
  bin/
  debug/
  PROVENANCE.env
  SHA256SUMS
  server.toml
  radixdb.service
  install-systemd.sh
  uninstall-systemd.sh
  start.sh
  status.sh
  stop.sh
  smoke-client.sh
```

В bundle также входят лицензия, notices, средства backup/restore и общие scripts
управления процессом. Переносите его как единый artifact и проверяйте checksum
на целевом сервере до установки.

## Установка в `/opt/radixdb`

Для системной установки tracked installer запускается от root. Он создаёт
системную учётную запись `radixdb`, копирует бинарники, устанавливает unit,
перечитывает конфигурацию systemd и включает службу. Включение не запускает её.

```sh
cd dist
sha256sum -c SHA256SUMS
sudo ./install-systemd.sh
sudo systemctl start radixdb
sudo systemctl status radixdb --no-pager
```

Получается следующий layout:

```text
/opt/radixdb/
  bin/
    radixdb-server
    radixdb-password
    radixdb-cli
    radixdb-smoke-client
  data/
  logs/
  run/
  server.toml
```

Служба работает как `radixdb:radixdb`. Бинарники остаются во владении root.
Installer создаёт `server.toml` с mode `0640` и владельцем `root:radixdb`, а
каталоги data, logs и run с mode `0750`. Никогда не помещайте в конфигурацию
пароль открытым текстом. Необязательный verifier пароля root также является
учётными данными и требует этих ограниченных прав доступа.

Другой абсолютный prefix передавайте installer и последующим административным
командам. Путь не может быть `/` и не должен содержать компоненты `.` или `..`.

```sh
sudo RADIXDB_INSTALL_PREFIX=/srv/radixdb ./install-systemd.sh
```

Сгенерированный unit назначает выбранный prefix рабочим каталогом. Поэтому
упакованный относительный `data_dir = "data"` разрешается внутри этого prefix.

## Установка native extension packages

Native extensions распространяются отдельными immutable packages и не
встраиваются в server release bundle. Соберите их командой
`cargo radixdb-plugin package`, перенесите каждый полный package directory в
operator-owned location, например `/opt/radixdb/plugins/`, затем перечислите
точный directory в `[plugins]` файла `server.toml`. При restart все packages
допускаются до открытия listener.

Не копируйте отдельный `.so` в `bin/` или data root. Package manifest, codec
vectors, compatibility report и provenance участвуют в admission и backup.
До добавления trusted native code к service прочитайте
[«Установку и эксплуатацию extensions»](../extensions/).

## Проверка установки

Endpoint по умолчанию доступен только на loopback: `127.0.0.1:15441`. Сначала
проверьте готовность процесса и протокола:

```sh
sudo systemctl status radixdb --no-pager
./smoke-client.sh
journalctl -u radixdb -n 100 --no-pager
```

С неизменённым release template smoke client проверяет handshake, беспарольную
loopback-аутентификацию `root`, server status и build identity. Он не умеет
передавать настроенный пароль root, не открывает каждую базу и не доказывает
готовность приложения. Настройте штатный доступ root установленной утилитой
`bin/radixdb-password` и проверьте обычным клиентом по процедуре из главы
[«Аутентификация»](../authentication/). Выбор и готовность базы описаны в главе
[«Работа сервера»](../server/). Перед изменением release template прочитайте
полную [конфигурацию сервера](../configuration/).

## Повторная установка и удаление бинарников

Обычная повторная установка сохраняет существующий `server.toml`. Указывайте
`RADIXDB_INSTALL_REPLACE_CONFIG=1` только для намеренной замены, предварительно
сохранив старый файл и проверив новые значения.

Обычное удаление выключает unit и удаляет четыре исполняемых файла, но сохраняет
конфигурацию и данные:

```sh
cd dist
sudo ./uninstall-systemd.sh
```

`RADIXDB_UNINSTALL_PURGE=1` удаляет весь prefix установки вместе с данными.
Не используйте этот режим как обычную команду обновления. Процедуры backup,
restore и upgrade описываются отдельно от установки.

Release gate проверяет dry-run systemd-установки в изолированном каталоге; это
не заменяет приёмку настоящего systemd, файловой системы и backup destination
на целевом сервере.
