---
title: История выпусков
description: Пользовательские изменения, идентичность и границы совместимости выпусков RadixDB.
---

Эта страница кратко описывает пользовательские выпуски. Источниками release
identity являются Git tags и `CHANGELOG.md`; подробный контракт находится в
тематических главах.

## 1.2 - в разработке

Руководство 1.2 следует текущей ветке исходников 1.2 и wire protocol 17. Пока
release metadata не завершены, собранные из неё бинарники продолжают сообщать
версию Cargo package 1.1.0; отличайте их от выпуска 1.1 по полной build identity.

### Безопасность сервера и управление доступом

Штатный сервер аутентифицирует долговечные субъекты базы, проверяет Argon2id
verifier пароля и право базы `CONNECT` до допуска session. Вместо plaintext TCP
можно включить прямой TLS с проверкой CA и имени сервера, строгими правами
private key и атомарной перезагрузкой сертификатов для новых подключений. Вход
`root` без пароля ограничен plaintext loopback и предназначен для восстановления,
если verifier root не настроен. Новая утилита `radixdb-password` создаёт
проверяемый Argon2id PHC verifier; после его настройки исходный пароль обязателен
для `root` на plaintext или TLS endpoints, а беспарольный вход отключён везде.

Субъекты и роли поддерживают enable, disable, rename и удаление с учётом
зависимостей. Права объектов и столбцов поддерживают `WITH GRANT OPTION` и
`REVOKE GRANT OPTION FOR`; делегирование роли хранит grantor, а schema `USAGE`
отделено от schema `CREATE`. При подключении и каждом запуске trigger проверяются
права schema и вызываемой функции.

### Серверное программирование

Functions, Procedures, Triggers и Jobs получили явные команды alter и drop.
RadixDB PL предоставляет typed context текущего и effective principal,
транзакции, запроса, времени оператора и Job. `SQL_IDENTIFIER(TEXT)` безопасно
формирует identifier для dynamic SQL, а `:OLD.column` и `:NEW.column` связывают
trigger records в static SQL.

Штатный сервер запускает долговечные scheduled Jobs с lease, запретом
параллельного выполнения одной задачи, retry с ограниченным exponential
backoff, coalescing пропущенных запусков, ограниченной историей и чистой
остановкой. Ошибки попыток имеют устойчивый диагностический класс, поэтому
приложениям не требуется разбирать текст ошибки.

### Trusted native extensions

Версия 1.2 добавляет stable C ABI 1.0, безопасный Rust authoring SDK и
deterministic package tooling для operator-trusted in-process extensions.
Package, допущенный при startup, может предоставить bounded external scalar
types, native scalar и batch functions, binary operators, B-tree/hash/bitmap
operator classes и bounded planner support. SQL транзакционно связывает exports
с catalog 6.2, а protocol 17 сохраняет external type identity и codec revision
на wire.

Host сохраняет владение storage, WAL, MVCC, catalog mutation, index pages, ACL
и recovery. Packages загружаются только из exact absolute allowlist entries;
network install, hot reload, version ranges и `ALTER EXTENSION UPDATE`
отсутствуют. Proving extension `radixdb-spatial` проходит через public SDK с
fixed и variable geometry types, native predicates, Morton-key B-tree index и
residual recheck.

Bundled CLI workflows backup и logical export пока не загружают plugin allowlist
и потому не являются поддержанным recovery path для extension-bound database.

Начните с [установки extensions](../../administration/extensions/), затем
используйте [руководство разработчика](../../programming/native-extensions/) и
[`cargo radixdb-plugin`](../../reference/programs/cargo-radixdb-plugin/).

### Надёжность и эксплуатация

Внешний physical backup записывает покрытые checksum сведения о сборке,
формате, базе и точном snapshot и восстанавливает только этот snapshot.
Логический export использует нормализованный checksum содержимого; dump можно
воспроизвести, импортировать в новый root, повторно открыть и экспортировать.
Runtime status артефактов ограничен по объёму и сообщает о truncation.

CLI сохраняет уровень изоляции и savepoints в batch, откатывает ошибочный batch
и показывает effective durability configuration. Снятые настройки cache и
compression теперь отклоняются, а не игнорируются. Nullable indexed window
partitions сохраняют группу `NULL` после cold storage и повторного открытия.

Protocol 17 не совместим по wire с protocol 14. Обновляйте server и client как
один проверенный комплект, а при пересечении неподдерживаемой границы
физического формата используйте logical export/import.

## 1.1.0 - 2026-09-08

RadixDB 1.1.0 определяется аннотированным тегом `v1.1.0` и использует wire
protocol 14. Этот выпуск заложил первую опубликованную процедурную и ACL-основу,
описанную ниже. Ограничения раздела относятся к 1.1, а не к текущей ветке 1.2.

### Процедурная основа базы

Версия 1.1 добавляет долговечные catalog objects для principals, roles, ACL
entries, functions, procedures, triggers и jobs. Ограниченный procedural
runtime использует существующие SQL parser, executor, transaction owner и путь
MVCC/WAL. Реализованы typed calls, local variables, control flow, cursors,
exception regions, динамический `EXECUTE`, транзакционные DML triggers и
долговечные записи попыток jobs.

Контексты invoker/definer и права roles/objects проверяются в пути выполнения
БД. Прикладная запись может атомарно публиковать audit и outbox rows, а
ограниченное публичное чтение ORM использует ту же authority. См.
[PL/SQL](../../programming/pl-sql/), [процедуры](../../programming/routines/),
[триггеры](../../programming/triggers/), [задания](../../programming/jobs/) и
[контроль доступа](../../administration/access-control/).

### Усиление хранения и совместимости

Normal rollback остаётся вне synchronous durability deadline, но ошибки записи
rollback marker передаются вызывающему коду. Publisher ownership отзывается при
закрытии, а same-process lock handoff усилен.

База с catalog 6.0 открывается без автоматической перезаписи. Первая
procedural- или ACL-DDL атомарно переводит каталог в exact minor 6.1. Обратного
writer path нет, а бинарник, знающий только 6.0, отклоняет 6.1 fail closed.
Сетевым контрактом этого выпуска остаётся protocol 14.

Перед сменой бинарника изучите [обновление](../../administration/upgrading/),
а поддержку SQL проверяйте по [матрице](../compatibility/), не предполагая
совместимость синтаксиса или wire protocol PostgreSQL.

### Принятое evidence и ограничения

Release gate сохранил checksum и access paths набора из 100 000 000 строк.
Наибольшая указанная регрессия запроса равна 4,91%, что ниже фиксированного
коридора 1,20 раза. Точная методика и привязка к исходникам находятся в
[тестах производительности](../benchmarks/).

Штатный сервер 1.1 остаётся loopback-only и не предоставляет principal login
или транспортное шифрование. ACL применим через доверенный embedded-контекст
или аутентифицированный gateway, но не является прямой аутентификацией
недоверенного TCP-клиента. Штатный сервер также не запускает фоновый Job
scheduler. Это явные границы продукта из глав [аутентификации](../../administration/authentication/)
и [заданий](../../programming/jobs/), а не возможности, подразумеваемые catalog
model.

## 1.0.0 - 2026-09-07

Версия 1.0.0 зафиксировала production-архитектуру catalog-artifact V6 и приняла
свидетельства 100M NVMe, 20k memory и шестичасового HDD run. Также появился
transport-independent ORM v1 с versioned IR, canonical JSON, стабильными schema
descriptors, typed parameters и расширениями выполнения embedded/TCP.

Существующие данные прежних unnamed-catalog layouts не переписываются на месте.
Миграция выполняется явным logical export/import в отдельно проверенное место
назначения. Перед ней изучите [обновление](../../administration/upgrading/) и
[резервное копирование](../../administration/backup-restore/).

## Чтение истории выпусков

Принадлежность выпуску не превращает распознанную parser форму в поддерживаемый
SQL-контракт. Независимо проверяйте целевой выпуск, compatibility matrix и
описанные ограничения.
