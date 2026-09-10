# Rust client examples для RadixDB

Эти примеры используют публичный crate `radixdb-client` и подключаются к
запущенному RadixDB TCP server.

Проверка сборки из корня репозитория:

```bash
cargo check --manifest-path examples/public/rust-client/Cargo.toml
```

Запуск против уже работающего локального сервера:

```bash
cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin basic -- \
  127.0.0.1:15441 radixtrade_client_demo

cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin parameters -- \
  127.0.0.1:15441 radixtrade_client_demo

cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin import_radixtrade -- \
  127.0.0.1:15441 radixtrade_tcp_import_demo examples/public/radixtrade
```

`import_radixtrade` — TCP client/server версия учебного импорта. Пример через
binary protocol импортирует `examples/public/radixtrade/schema.sql` и
`seed-small.sql`, затем проверяет количество строк и metadata индексов.

Или запуск временного локального сервера и всех примеров:

```bash
examples/public/rust-client/scripts/smoke.sh
```

Опциональные credentials берутся из:

- `RADIXDB_LOGIN` — default `root`;
- `RADIXDB_PASSWORD` — не передаётся, если пустой или не задан.

Текущий authentication — protocol placeholder: сервер требует authenticate
message, но ещё не проверяет credentials. Держите тестовые серверы на loopback
или внутри доверенной локальной границы.
