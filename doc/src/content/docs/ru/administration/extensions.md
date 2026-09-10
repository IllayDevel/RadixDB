---
title: Установка расширений
description: Установка доверенных нативных пакетов, привязка к базе и безопасная эксплуатация жизненного цикла.
---

RadixDB 1.2 загружает доверенные пакеты нативных расширений при запуске
сервера. Оператор размещает пакет на узле и явно добавляет его абсолютный
каталог в allowlist. Затем владелец базы связывает выбранные экспорты с базой
через SQL.

Расширение выполняется внутри `radixdb-server` с правами его процесса. Это не
sandbox. Устанавливайте только пакеты, исходникам, среде сборки и provenance
которых вы доверяете.

## Пакет и объекты базы

Пакет и SQL-объекты имеют разные жизненные циклы:

1. **Пакет** является проверенным каталогом с shared library, manifest,
   векторами codec, отчётом совместимости и provenance сборки.
2. **Привязка расширения** фиксирует в каталоге одной базы точные identity и
   версию пакета, диапазон ABI и fingerprint descriptor.
3. External types, native functions, operators, operator classes и planner
   support явно создаются из экспортов этой привязки.

SQL не принимает путь к библиотеке или URL и не загружает код из сети. До
успешного `CREATE EXTENSION` пакет уже должен находиться в неизменяемом реестре
процесса.

## Требования платформы

Первая пакетная платформа имеет target `x86_64-unknown-linux-gnu`. Допускается
x86-64 little-endian ELF shared object, не требующий символов glibc новее 2.36,
использующий plugin ABI 1.0 и содержащий официальную аттестацию
`panic = "unwind"`. Официальный release tool собирает пакеты Rust 1.97.0 в
`rust:1.97.0-bookworm`.

Manifest ограничен 64 KiB, shared library — 256 MiB. Каталог пакета, manifest,
каталог `lib` и библиотека должны принадлежать root либо effective server user
и не разрешать запись группе или остальным пользователям. Symlink отклоняется.

## Размещение и allowlist пакета

Полный пакет имеет следующую структуру:

```text
radixdb-pair-1.0.0/
  radixdb-plugin.toml
  radixdb-plugin-golden.toml
  radixdb-plugin-provenance.toml
  radixdb-plugin-compatibility.toml
  lib/
    libradixdb_pair.so
```

Скопируйте полный каталог, не изменяя его файлы, затем назначьте ограниченные
ownership и permissions. Сам путь назначения должен быть абсолютным,
нормализованным и не быть symlink.

```sh
sudo install -d -o root -g radixdb -m 0750 /opt/radixdb/plugins
sudo cp -a dist/radixdb-pair-1.0.0 /opt/radixdb/plugins/
sudo chown -R root:radixdb /opt/radixdb/plugins/radixdb-pair-1.0.0
sudo chmod -R go-w /opt/radixdb/plugins/radixdb-pair-1.0.0
```

Добавьте верхнеуровневую таблицу `[plugins]` в `server.toml`. Каждый элемент
указывает ровно на один каталог пакета; сервер не сканирует родительские
каталоги.

```toml
[plugins]
package_directories = [
  "/opt/radixdb/plugins/radixdb-pair-1.0.0",
]
```

Перезапустите процесс. Загрузить, перезагрузить или выгрузить plugin в
работающем процессе нельзя.

```sh
sudo systemctl restart radixdb
journalctl -u radixdb -n 100 --no-pager
```

До открытия listener сервер проверяет все настроенные пакеты как единый набор:
пути и права, границы manifest, ELF target, SHA-256, ABI, descriptor fingerprint
и все ссылки descriptor. Любая ошибка прекращает запуск; частичный реестр не
публикуется. Успешный запуск сообщает generation реестра и количество packages,
types, functions, operators, operator classes и planner support.

`--print-endpoint` только разбирает конфигурацию и завершается до admission
пакетов. Проверяйте загрузку реальным foreground-запуском или запуском службы.

## Привязка экспортов к базе

Запустите install script пакета от владельца базы либо `root`. Поместите
связанные объекты в одну транзакцию, чтобы опубликовался весь граф или ни одна
его часть.

```sql
BEGIN;
CREATE EXTENSION radixdb_pair VERSION '1.0.0';
CREATE TYPE public.pair FROM EXTENSION radixdb_pair AS 'pair';
CREATE FUNCTION public.pair_sum(value public.pair NOT NULL)
RETURNS INTEGER NOT NULL LANGUAGE NATIVE
FROM EXTENSION radixdb_pair AS 'pair_sum';
COMMIT;
```

SQL-имена могут отличаться от local export IDs. Local ID в `AS` и версия
пакета являются byte-exact стабильными identity; не выводите их из SQL-имён.
`IF NOT EXISTS` успешен только для byte-equivalent существующей привязки.

`DESCRIBE DATABASE` добавляет `radixdb.plugin_requirements.v1` в extensions
описателя. Объект содержит каждую привязку, identity внешнего типа, версию
codec и предел payload. Используйте его для генерации и проверки клиентских
адаптеров.

## Доступ и выполнение

Создать или удалить extension может владелец базы либо `root`. Для зависимых
объектов также нужны ownership привязки и `CREATE` в целевой schema. Вызов
native function использует обычную видимость schema и право Function
`EXECUTE`; operator проверяет право его backing function.

Авторизация завершается до входа в callback расширения. Callback не получает
current principal, ACL bypass, handle изменения catalog или внутренности
storage.

Внешние значения передаются по protocol 17 как stable type object ID, версия
codec и ограниченные canonical bytes. Они не заменяются молча на `BYTES`.
Клиент без согласованной capability `ExternalValueV1` получает ошибку
неподдерживаемого типа.

## Смена версии и удаление

Один процесс сервера выбирает только наибольший настроенный SemVer для каждого
package UUID. Привязка базы фиксирует точную версию. Не добавляйте новую версию
на сервер, который должен продолжать открывать базы со старой привязкой:
старый artifact станет shadowed, а такие базы перейдут в restricted mode.

В RadixDB 1.2 нет hot reload и команды обновления на месте
`ALTER EXTENSION UPDATE`. Сохраняйте каждый exact package, необходимый базе, и
считайте смену версии явной миграцией приложения с отдельно проверенным
rollback. Package tool сравнивает новый artifact через `--previous-package`,
но compatible report не изменяет catalog binding автоматически.

Удаляйте зависимые объекты в обратном порядке, затем удаляйте привязку:

```sql
BEGIN;
DROP FUNCTION public.pair_sum(public.pair) RESTRICT;
DROP TYPE public.pair RESTRICT;
DROP EXTENSION radixdb_pair RESTRICT;
COMMIT;
```

`CASCADE` для объектов расширения в 1.2 не поддерживается. `RESTRICT` запрещает
удаление, пока от экспорта зависит таблица, функция, operator, index или другой
catalog object.

## Отсутствующий или несовместимый пакет

Если catalog базы требует отсутствующий package, codec или semantic revision,
только эта база переходит в restricted diagnostic mode. Обычный SQL и decoding
external values закрываются с ошибкой; остальные базы того же сервера остаются
независимыми.

Верните точный допустимый пакет и перезапустите сервер. Если у привязки нет
зависимостей, `root` может удалить её командой `DROP EXTENSION ... RESTRICT`
даже в restricted mode. Не редактируйте catalog или data files для обхода
проверки.

Physical backup базы не содержит каталоги пакетов. Сохраняйте точные package
artifacts, checksums и provenance рядом с записями backup. Bundled CLI workflows
backup и logical export пока не принимают plugin allowlist и потому не являются
поддержанным recovery path для extension-bound database. До production
эксплуатации создайте и проверьте отдельную процедуру; см.
[«Backup и восстановление»](../backup-restore/).

Далее: [разработка нативных расширений](../../programming/native-extensions/)
или точный [SQL-справочник расширений](../../reference/sql/extensions/).
