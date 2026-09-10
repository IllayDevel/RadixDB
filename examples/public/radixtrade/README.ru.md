# Публичные SQL-примеры RadixTrade

RadixTrade Group — общий учебный домен для публичной документации RadixDB.
Это вымышленная торговая компания: филиалы, склады, поставщики, клиенты,
товары, заказы, оплаты, отгрузки и события приложения.

Первый шаг всегда один: импортировать схему и seed-данные. Все query examples
рассчитаны на уже импортированную базу.

## Запуск

Из корня репозитория:

```bash
examples/public/radixtrade/scripts/01-import-schema.sh
examples/public/radixtrade/scripts/02-run-query-tour.sh
examples/public/radixtrade/scripts/03-partial-index-reject-probe.sh
```

Скрипты сначала используют `/opt/radixdb/bin/radixdb-cli`, если он установлен.
Если установленного CLI нет, используется fallback:

```bash
cargo run -q --bin radixdb-cli --features cli --
```

По умолчанию создаётся локальная file-база в
`examples/public/radixtrade/runtime/`. Этот каталог игнорируется Git.

DSN можно переопределить:

```bash
RADIXTRADE_DB_DSN='file:///tmp/radixtrade-demo?sync_mode=none' \
  examples/public/radixtrade/scripts/01-import-schema.sh
```

## Файлы

- `schema.sql` — DDL учебной базы.
- `seed-small.sql` — маленький детерминированный набор данных.
- `queries/` — учебный тур: select, filters, joins, aggregates, DML,
  indexes, UUID, transactions и optimistic update.
- `server/server.toml` — минимальный пример server config.
- `maintenance/` — запускаемый maintenance smoke; обслуживание идёт после
  схемы и запросов.

Rust TCP client examples лежат рядом с этим SQL tutorial:

- `examples/public/rust-client/basic.rs`;
- `examples/public/rust-client/parameters.rs`.
