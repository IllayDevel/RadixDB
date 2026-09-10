# Пример расширения RadixDB на Rust

Этот самостоятельный `cdylib` является минимальным публичным примером
нативного расширения RadixDB 1.2. Он объявляет один фиксированный внешний тип и
одну нативную скалярную функцию через безопасный SDK `radixdb-plugin`.

Запустите локальные проверки автора из корня репозитория:

```sh
cargo run --locked -p cargo-radixdb-plugin -- check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
cargo run --locked -p cargo-radixdb-plugin -- test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

Создание распространяемого пакета дополнительно требует официальной среды
сборки `rust:1.97.0-bookworm`. Перед установкой доверенного кода в процесс
сервера прочитайте главы руководства о нативных расширениях и инструментах
упаковки.
