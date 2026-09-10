# Участие в разработке RadixDB

[English](CONTRIBUTING.md)

RadixDB 1.1.0 является последним принятым выпуском. Изменения после его
аннотированного тега считаются невыпущенными до фиксации следующего выпуска.
Вклад должен сохранять correctness, durability и документированные контракты.
История выпусков находится в [CHANGELOG.ru.md](CHANGELOG.ru.md).

## Перед изменением кода

- Прочитайте [README.ru.md](README.ru.md) и текущие ограничения.
- Используйте [каталог документации](doc/README.ru.md), чтобы найти структуру
  руководства, команды проверки и публичный архив доказательств.
- Несоответствия parser/executor/documentation записывайте в issue с
  воспроизведением и критериями приёмки.
- Публичные утверждения о SQL подтверждайте кодом, тестами или исполняемыми
  примерами.

## Команды разработки

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --features cli,bench-harness
cargo check --locked --workspace --tests --bins --features cli,bench-harness
cargo test --locked -p radixdb-client
```

Полезные профильные тесты:

```bash
cargo test --test data_type_contract_test -- --nocapture
cargo test --test partial_index_tcp_client_test -- --test-threads=1 --nocapture
cargo test --test pragma_test -- --nocapture
```

Тяжёлые benchmarks и разрушительные/recovery tests запускайте только в явно
выделенном каталоге тестовых данных, а не в каталоге с важными данными.

## Стиль коммитов

- Делайте сфокусированные коммиты.
- По возможности отделяйте изменения только документации от изменений кода.
- Не коммитьте сгенерированные базы, WAL, benchmark CSV, логи, release binaries
  или локальные конфигурации с секретами.

## Лицензирование

Участники сохраняют copyright на свои вклады. До принятия вклада участник
должен зафиксировать согласие с [CLA.md](CLA.md). CLA предоставляет licensing
steward права, необходимые для распространения компонентов по карте
[LICENSING.md](LICENSING.md) и выдачи отдельных коммерческих лицензий.
Русское пояснение находится в [CLA.ru.md](CLA.ru.md).
