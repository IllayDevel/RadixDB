---
title: Встраиваемый Rust
description: Открытие RadixDB в Rust-процессе, параметры, чтение строк и управление транзакциями.
---

Встраиваемый интерфейс запускает движок базы внутри процесса приложения.
`radixdb-server` ему не нужен, но процесс должен владеть каталогом базы и её
жизненным циклом. В этой главе описан публичный API 1.2 из верхнеуровневого
крейта `radixdb` и его модуля `radixdb::api`.

## Подключение крейта

При сборке из исходного дерева укажите корень workspace. Замените путь из
примера на каталог исходников 1.2, который использует приложение:

```toml
[dependencies]
radixdb = { path = "/path/to/RadixDB" }
```

Проверочный проект этого руководства привязывает путь к коммиту
`23bf35df011aae6816d77578be96074b02bc363c` и собирается offline с lockfile.

## Открытие базы

`Database::open_in_memory()` создаёт отдельный временный движок в процессе.
`Database::open("memory://name")` использует канонический реестр и разделяет
движок с повторным открытием того же DSN. Для постоянной базы нужен file DSN:

```rust
use radixdb::api::Database;

let db = Database::open(
    "file:///srv/radixdb/app?sync_mode=full&checkpoint_on_close=off",
)?;
```

Повторное открытие того же канонического file DSN в одном процессе возвращает
ещё один handle общего движка. Несовместимая конфигурация уже открытого DSN
отклоняется. Не открывайте один каталог базы из разных процессов.

## Команды и параметры

`execute()` возвращает число затронутых строк. Позиционные placeholders имеют
вид `$1`, `$2` и далее. Tuple или `params!` передаёт позиционные значения,
а `execute_named()` вместе с `named_params!` передаёт значения `:name`:

```rust
use radixdb::{named_params, params};

db.execute(
    "INSERT INTO notes (id, title) VALUES ($1, $2)",
    params![1_i64, "first"],
)?;
db.execute_named(
    "INSERT INTO notes (id, title, done) VALUES (:id, :title, :done)",
    named_params! { id: 2_i64, title: "second", done: true },
)?;
```

Поддерживаются целые и вещественные числа, Boolean, строки, bytes, дата и время,
UUID, JSON, decimal, vectors и `Option<T>`. Binding отделён от SQL-текста.
Не собирайте значения через интерполяцию строк.

`prepare()` разбирает переиспользуемый statement. `Statement` выполняет команду
или запрос с новым набором параметров. Он удерживает ссылку на владельца, поэтому
освободите statements и rows до явного закрытия базы.

## Чтение результатов

`query()` возвращает `Rows`. Iterator выдаёт `Result<ResultRow>`, потому что
ошибка может появиться уже после начала запроса:

```rust
let rows = db.query(
    "SELECT id, title FROM notes WHERE id >= $1 ORDER BY id",
    (1_i64,),
)?;
for row in rows {
    let row = row?;
    let id: i64 = row.get(0)?;
    let title: String = row.get_by_name("title")?;
    println!("{id}: {title}");
}
```

Позиции столбцов начинаются с нуля. `get_by_name()` не учитывает регистр и
отклоняет неоднозначное повторяющееся имя; задайте qualification или alias.
Используйте `query_one::<T, _>()` для ровно одного scalar value,
`query_opt::<T, _>()` для нуля или одного и `query_as::<T, _>()` с реализацией
`FromRow` для прикладных записей.

Интерфейс `Rows::advance()` с меньшим числом allocation возвращает `false` и
при EOF, и после отложенной ошибки выполнения или закрытия. После `false`
вызовите `Rows::error()`. Iterator возвращает такую ошибку отдельным item.
`Rows::close()` завершает чтение раньше; drop курсора также закрывает его.

## Транзакции

Управляйте транзакцией через handle, а не SQL-текст:

```rust
let mut transaction = db.begin()?;
transaction.execute(
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
    (100_i64, 7_i64),
)?;
transaction.rollback()?;
```

`begin()` использует read committed. `begin_with_isolation()` также открывает
snapshot isolation. Handle предоставляет `commit()`, `rollback()`, `savepoint()`,
`rollback_to_savepoint()` и `release_savepoint()`. Drop незавершённой транзакции
выполняет rollback.

## Ошибки и закрытие

Все вызовы возвращают `radixdb::Result`. Приложение должно обрабатывать ошибки
constraints, типов, parser, storage и транзакций; ошибка не является пустым
результатом. До `Database::close()` освободите все `Rows`, `Statement` и handle
транзакции:

```rust
db.close()?;
```

Явное закрытие останавливает движок и освобождает file lock. Оно завершится
ошибкой, пока существует другой handle базы, транзакция или удерживаемый
владелец. Обычный `Drop` закрывает движок после исчезновения последнего handle,
но явный close полезен, когда процесс должен подтвердить освобождение каталога.

Полный исполняемый пример находится в
`doc/examples/clients/embedded.rs`. Проверка документации создаёт постоянную
временную базу, проверяет UNIQUE error и rollback, затем явно закрывает базу.

Если движок должен работать в отдельном серверном процессе, перейдите к
[Rust TCP-клиенту](../rust-client/).
