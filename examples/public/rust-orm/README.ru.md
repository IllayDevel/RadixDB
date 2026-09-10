# RadixDB Rust ORM: полное приложение RadixTrade

[English version](README.md)

Это самостоятельный пример полного жизненного цикла generated ORM на базе
учебной торговой компании RadixTrade:

1. реальная схема и данные создаются через обычное TCP-соединение;
2. из БД явно выгружается versioned schema descriptor;
3. offline codegen генерирует типизированные Rust-модели;
4. приложение выполняет generated CRUD, references и navigation;
5. ORM и прямой SQL работают в одной транзакции на одном соединении.

Используется существующая полная схема
[`../radixtrade`](../radixtrade/README.ru.md): филиалы, покупатели, справочники
товаров, заказы и строки заказов.

## Запуск сервера

Пример использует отдельный порт и отдельный каталог данных, не затрагивая
`/opt/radixdb`:

```bash
cargo build --locked --bin radixdb-server
target/debug/radixdb-server --config examples/public/rust-orm/server.toml
```

Параметры по умолчанию:

| Параметр | Значение | Переменная |
|---|---|---|
| Сервер | `127.0.0.1:16441` | `RADIXDB_ADDRESS` |
| База | `radixtrade_orm_demo` | `RADIXDB_DATABASE` |
| Логин | `root` | `RADIXDB_LOGIN` |
| Пароль | отсутствует | `RADIXDB_PASSWORD` |

## Полный цикл одной командой

```bash
examples/public/rust-orm/run-complete-demo.sh
```

Команда последовательно выполняет все четыре стадии ниже.

## 1. Создание схемы и тестовых данных

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin bootstrap
```

`bootstrap` выполняет `../radixtrade/schema.sql` и
`../radixtrade/seed-small.sql`. Повторный запуск пересоздаёт только таблицы
выбранной учебной базы.

## 2. Экспорт descriptor

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin export_schema
```

Результат:

```text
examples/public/rust-orm/schema/radixtrade.schema.json
```

## 3. Offline codegen

```bash
cargo run --locked -p radixdb-orm --bin radixdb-orm-codegen -- \
  examples/public/rust-orm/schema/radixtrade.schema.json \
  examples/public/rust-orm/generated_schema.rs
```

Генератор не подключается к серверу. Он получает только сохранённый descriptor
и создаёт records, typed columns, `Reference<T>`, ключи и schema fingerprint.

В tutorial descriptor и generated source игнорируются Git, поскольку catalog
identity относится к локальной базе. В реальном приложении оба файла следует
проверять и хранить в VCS.

## 4. Generated-приложение

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --features generated-app \
  --bin trading_company
```

В [`trading_company.rs`](trading_company.rs) показаны:

- `RtCustomers::new()` и типизированный `insert()`;
- ссылки на группу покупателя, филиал, заказ и товар;
- `RtSalesOrders::get()` и `save()`;
- транзитивный путь
  `order_line -> sales_order -> customer -> name`;
- navigation вместе с `SUM`/`GROUP BY`;
- вывод ORM JSON IR и сгенерированного SQL;
- прямой SQL в той же транзакции и на том же `Connection`.

## Изменение схемы

После намеренного `ALTER TABLE` повторяются стадии 2 и 3, затем проверяется diff
descriptor/generated source. Старый generated CRUD завершится
`SchemaChanged`; скрытой регенерации или runtime-перепривязки нет.

Минимальный builder-only пример без сервера остаётся доступен:

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin orm_quickstart
```

Полный контракт описан в [руководстве ORM](../../../doc/src/content/docs/ru/clients/orm.md).
