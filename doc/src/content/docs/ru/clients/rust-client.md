---
title: Rust TCP-клиент
description: Подключение к протоколу RadixDB 17, prepared statements и безопасная работа с typed values.
---

`radixdb-client` является синхронным Rust-клиентом бинарного протокола RadixDB.
Он не содержит движок базы и не является PostgreSQL-клиентом. Базовая версия
документации 1.2 согласует protocol 17.

## Подключение и выбор базы

До выполнения SQL создайте соединение и пройдите аутентификацию в выбранной
базе. Субъекту нужны право базы `CONNECT` и объектные права приложения.

```rust
use std::time::Duration;
use radixdb_client::Connection;

let timeout = Duration::from_secs(5);
let mut connection = Connection::connect_with_timeouts(
    "127.0.0.1:15441",
    timeout,
    timeout,
    timeout,
)?;
connection.authenticate_database(
    "application",
    "application_reader",
    "secret",
)?;
```

`connect()` использует системные настройки socket. `connect_with_timeouts()`
задаёт отдельные пределы connect, read и write. Protocol handshake входит в
создание соединения и отклоняет несовместимый сервер.

Для endpoint с прямым TLS используйте `TlsConnection::connect_tls()` и
`TlsClientConfig`. Настроенный административный `root` входит через
`authenticate("root", Some(password))`; без verifier вход `root` без пароля
остаётся только восстановительным режимом plaintext loopback. См. главу
[«Аутентификация»](../../administration/authentication/).

## Команды и prepared statements

`execute()` возвращает `ExecuteResult::CommandComplete` для команд и
`ExecuteResult::Cursor` для statements со строками. Передавайте параметры
отдельно, не вставляйте их в SQL интерполяцией:

```rust
use radixdb_client::{ExecuteResult, WireValue};

let insert = connection.prepare(
    "INSERT INTO notes (id, title) VALUES ($1, $2)",
)?;
let result = connection.execute_prepared(
    &insert,
    vec![WireValue::Int(1), WireValue::String("first".to_string())],
)?;
assert!(matches!(result, ExecuteResult::CommandComplete { .. }));
connection.close_prepared(insert)?;
```

`execute_with_positional_parameters()` принимает `Vec<WireValue>`.
`execute_with_parameters()` принимает `BTreeMap<String, WireValue>` для
именованных параметров, а `execute_with_bindings()` принимает оба набора.
Prepared handle принадлежит создавшему его соединению; другое соединение
получит `PreparedStatementOwnerMismatch`.

## Values native extension

Protocol 17 представляет external value через stable type object ID, ненулевой
codec version и bounded canonical bytes. `radixdb-client` повторно экспортирует
эту форму как `WireValue::External`:

```rust
let mut payload = Vec::with_capacity(16);
payload.extend_from_slice(&1_i64.to_le_bytes());
payload.extend_from_slice(&2_i64.to_le_bytes());
let pair = WireValue::External {
    type_object_id: [
        0xda, 0xa5, 0xe8, 0x3d, 0x0e, 0xa3, 0x3c, 0x36,
        0xca, 0xde, 0x62, 0x86, 0x2b, 0xae, 0xef, 0x54,
    ],
    codec_version: 1,
    payload,
};
```

Берите identity, codec revision и payload bounds из `DESCRIBE DATABASE` либо
generated plugin metadata. Не выводите их из SQL type name и не заменяйте value
на `WireValue::Bytes`. High-level ORM намеренно отклоняет external value, если
его canonical codec не обслуживает plugin-aware adapter.

## Курсоры

Курсор является stream уровня соединения. До несвязанной команды прочитайте его
до EOF, закройте или отмените:

```rust
let ExecuteResult::Cursor(cursor) = connection.execute(
    "SELECT id, title FROM notes ORDER BY id",
)? else {
    return Err("SELECT did not open a cursor".into());
};
loop {
    let batch = connection.fetch(&cursor)?;
    for row in batch.rows {
        println!("{:?}", row.values);
    }
    if batch.eof {
        break;
    }
}
```

Новая команда при активном курсоре возвращает
`ClientError::CommandsOutOfSync`. `close_cursor(cursor)` и `cancel(cursor)`
потребляют handle курсора. `fetch_batch()` может запросить columnar transport;
protocol 17 вправе вернуть документированный row fallback, если запрос для него
не подходит. Не смешивайте режимы fetch у одного курсора.

## Транзакции

Транзакциями управляют отдельные protocol operations:

```rust
connection.begin()?;
connection.execute(
    "UPDATE accounts SET balance = balance - 100 WHERE id = 7",
)?;
connection.rollback()?;
```

`begin_with_isolation()` явно выбирает `ReadCommitted` или `Snapshot`.
`commit()`, `rollback()`, `savepoint()`, `rollback_to_savepoint()` и
`release_savepoint()` обновляют состояние, отслеживаемое клиентом. Общие
`execute("BEGIN")`, `execute("COMMIT")` и `execute("ROLLBACK")` отклоняются
серверной границей.

## Ошибки и неопределённый результат

`ClientError::Server` означает явный ответ сервера. `is_retryable()` возвращает
true, только когда этот ответ классифицирует операцию как заведомо не
опубликованную. Для transport failures он возвращает false.

Если после отправки write или commit соединение потеряно либо наступил timeout,
но ответ не получен, outcome is unknown: сервер мог опубликовать операцию.
Удалите соединение из pool, подключитесь заново и сверьте прикладной idempotency
key или durable operation record до решения о повторе. Нельзя автоматически
повторять такую запись только по transport error.

Незавершённый transport round trip помечает соединение poisoned, а состояние
транзакции становится unknown. Для диагностики проверяйте `is_poisoned()`, а
перед возвратом соединения в pool проверяйте `is_reusable()`. Переиспользуемое
соединение открыто, не poisoned, не имеет активного курсора и активной либо
неопределённой транзакции.

Tokio `AsyncConnection` соблюдает те же правила владения. Он выполняет по одной
команде, не создавая скрытого multiplexing. Drop future после начала I/O
помечает соединение poisoned, потому что его результат уже нельзя безопасно
сопоставить.

## Завершение

До pool или shutdown дочитайте либо закройте курсор, завершите транзакцию и
закройте prepared statements. Затем явно закройте socket:

```rust
assert!(connection.is_reusable());
connection.shutdown()?;
```

Полный пример `doc/examples/clients/tcp.rs` проверяет prepared execution,
UNIQUE rejection, `CommandsOutOfSync`, cursor close и rollback на совместимой
сборке сервера.

Для descriptor-driven и generated models на том же соединении перейдите к
[главе ORM](../orm/) либо к
[«Разработке native extensions»](../../programming/native-extensions/) для
контракта external codec.
