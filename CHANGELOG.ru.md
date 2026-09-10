# История изменений

[English](CHANGELOG.md)

Этот файл содержит видимые пользователю изменения исходников и API.
Совместимость сетевого протокола и формата хранения определяется их отдельными
проверками версий.

## Не выпущено

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
