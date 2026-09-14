# История изменений

[English](CHANGELOG.md)

Этот файл содержит видимые пользователю изменения исходников и API.
Совместимость сетевого протокола и формата хранения определяется их отдельными
проверками версий.

## Не выпущено

## 1.2.21 - 2026-09-14

### Качество выпуска и поиск пакетов

- Усилены чувствительные к таймингам CI-проверки отмены TCP-запроса при разрыве
  соединения и финализации долговечного Job scheduler. Runtime-поведение, SQL,
  wire protocol, storage format и публичные API не изменились.
- Для crates.io добавлены отдельные тематические keywords движка, хранилища,
  исполнителя, клиента, ORM, application SDK, procedural runtime и пакетов
  native extensions.

## 1.2.19 - 2026-09-14

### Прикладные интерфейсы

- Добавлен предметно-независимый `radixdb-app-sdk` для доверенных Rust-сервисов:
  ограниченные request/session identity, deadlines, cancellation, лимиты
  результатов, стабильная классификация исходов и сгенерированные контракты,
  связанные со схемой.
- Добавлена явная привилегия базы `DESCRIBE` для получения схемы.
- Сохранена кардинальность табличных результатов процедур в generated contracts;
  добавлены детерминированные fingerprints таблиц, процедур и application events.
- Расширены контракты ORM для бинарных предикатов, NULL/BETWEEN/IN, подзапросов,
  CASE, tuple, сортировки и сквозной нумерации параметров.

### Типы SQL и ограничения

- Добавлен ограниченный `TEXT(n)` с вариантами записи `VARCHAR(n)` и `CHAR(n)`.
- Добавлена самостоятельная SQL- и catalog-идентичность `DOUBLE PRECISION` на f64.
- Добавлен `TIMESTAMP` для календарной даты и времени без неявного часового пояса.
- Добавлен `TIME` для времени суток.
- Добавлен `TIMESTAMPTZ` для моментов времени с проверяемым преобразованием через
  IANA/fixed-offset часовой пояс session и отклонением неоднозначного или
  несуществующего локального времени при переходе DST.
- Производные сложение, вычитание и умножение `DECIMAL` стали точными при разных
  масштабах, с проверками precision и overflow.
- Добавлены скалярные и упорядоченные составные первичные ключи, включая текстовые;
  каждый компонент не допускает NULL, уникальность проверяется по полному ключу.
- Приняты qualified relation names в post-load constraints, пакетные cold
  uniqueness probes при bulk insert/commit и разделение variable row groups по
  байтовому бюджету до публикации.

### Сервер, программирование и управление доступом

- Добавлены password-аутентификация catalog principals, проверка `CONNECT`,
  прямой TLS, перезагрузка сертификатов и `radixdb-password`. Беспарольный
  `root` остаётся только recovery-путём через plaintext loopback, если root
  verifier не настроен.
- Добавлены lifecycle principals/roles, grant options, grantor делегированной
  роли, независимые schema `USAGE`/`CREATE` и проверки прав trigger.
- Добавлены lifecycle-команды routines, triggers и jobs, typed contexts RadixDB
  PL, проверяемые dynamic identifiers и static trigger records `OLD`/`NEW`.
- Добавлен долговечный Job scheduler штатного сервера с leases, no-overlap,
  ограниченным retry/backoff, coalescing пропущенных запусков, историей и чистой
  остановкой.

### Доверенные native extensions

- Добавлены stable C ABI 1.0, безопасный Rust authoring SDK и deterministic
  package tooling для bounded external scalar types, scalar/batch functions,
  binary operators, B-tree/hash/bitmap operator classes и planner support.
- Добавлены exact checksummed startup allowlists, транзакционные catalog 6.2
  bindings, extension identity в protocol values и native packages для x86-64
  и AArch64 GNU/Linux с проверкой архитектуры ELF.
- Добавлен `radixdb-spatial` как proving extension для geometry values,
  predicates, Morton-key B-tree access и residual recheck.
- Extension packages остаются доверенным in-process кодом. Network install, hot
  reload, version ranges, `ALTER EXTENSION UPDATE`, aggregate/window/table
  descriptors и extension-aware bundled backup/export отсутствуют.

### Надёжность и совместимость

- Добавлены checksum-bound identity физических backup, проверка deterministic
  logical export и ограниченный runtime status артефактов.
- CLI сохраняет isolation/savepoints в batch; снятые cache/compression настройки
  теперь отклоняются, а не игнорируются.
- Усилена синхронизация последнего владельца CompactArc под ThreadSanitizer и
  проверка ABI native extensions на AArch64/QEMU.
- Wire protocol 18 не совместим с protocol 14 RadixDB 1.1.0; обновляйте server и
  clients вместе, а между несовместимыми physical formats используйте logical
  export/import.

## 1.1.0 - 2026-09-08

### Процедурная основа базы

- Добавлены долговечные объекты catalog 6.1 для principals, roles, ACL entries,
  functions, procedures, triggers и jobs с атомарным переходом `6.0 -> 6.1` и
  fail-closed обработкой старым бинарником.
- Добавлен проверенный ограниченный procedural runtime RadixDB поверх
  существующих SQL parser, executor и MVCC owner: typed calls, cursors,
  exception regions, динамический `EXECUTE`, транзакционные DML triggers и
  долговечные jobs.
- Добавлены авторизация invoker/definer, права roles и объектов, атомарные
  business/audit/outbox operations и ограниченное публичное чтение ORM.
- Добавлены проверки crash/reopen, publication failpoint, исчерпания ресурсов,
  cancellation, конкурентных DDL/DML/CALL/REVOKE и формата каталога.

### Усиление хранения и совместимости

- Normal rollback оставлен вне synchronous durability deadline с сохранением
  передачи ошибок rollback marker.
- Усилен отзыв publisher lock при закрытии и same-process lock handoff.
- Сохранены принятые cardinality, checksum и access paths на 100 миллионах
  строк; все измеренные release cases остались в фиксированном коридоре
  `1.20x`.

## 1.0.0 - 2026-09-07

### Базовый выпуск

- Принята production-архитектура catalog-artifact V6 и её канонические
  completion gates.
- Приняты correctness/performance evidence на 100M NVMe и memory profile 20k
  для micro-device.
- Принят CA-90.3 после полного 100M/6h run на Atom/HDD: 2 351 035 операций,
  2 100 успешных проверок инвариантов, финальные snapshot/restore/digest и
  чистое завершение процесса.
- Сохранено live recovery evidence при намеренном I/O starvation и
  незапланированном ATA reset `FLUSH CACHE EXT` на стенде с деградировавшим HDD.
- Испытания 24h/72h отложены в отдельную программу на исправном железе; они не
  блокируют этот release baseline.

### Клиентский ORM v1

- Добавлен transport-independent crate `radixdb-orm` с versioned IR, canonical
  JSON, JSON Schemas, детерминированным SQL rendering, typed parameters,
  dynamic records, GUI descriptors и offline-генерацией Rust-кода.
- Добавлены расширения ORM для существующих embedded database и Rust TCP
  clients. Raw SQL и ORM разделяют одного владельца соединения и транзакции.
- Добавлены детерминированные имена ограничений, стабильные identities,
  транзакционный `ALTER TABLE ... DROP CONSTRAINT` и полные JSON descriptors
  `DESCRIBE TABLE/DATABASE`.
- Добавлены owner-typed generated columns и descriptors одноколоночных
  PRIMARY/UNIQUE NOT NULL keys. `Reference<T>` остаётся key-only;
  cascade-save и reverse collections намеренно отсутствуют.
- Опубликованы language-neutral conformance fixtures для каждого принятого
  примера контракта ORM и отдельный пакет быстрого старта на Rust.
- Существующие unnamed-catalog данные не переписываются на месте. Миграция
  требует явного logical export/import в отдельно проверенное место назначения.
