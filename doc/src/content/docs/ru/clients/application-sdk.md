---
title: Application SDK
description: Создание доверенного прикладного Rust-сервиса со сгенерированными контрактами RadixDB, связанными со схемой.
---

`radixdb-app-sdk` является универсальной прикладной границей для доверенных
Rust-сервисов. Он объединяет асинхронный клиент RadixDB, документы ORM и
сгенерированный контракт базы, не открывая внутреннее устройство движка или
универсальный интерфейс базы браузерам.

```text
browser or API consumer
        -> application service and product authorization
        -> generated schema-bound Rust contract
        -> radixdb-app-sdk
        -> radixdb-client / radixdb-orm
        -> RadixDB server
```

Универсальный crate не содержит прикладных таблиц, процедур, маршрутов, ролей
или экранов. Сгенерированный прикладной crate относится к одной логической
схеме базы. Прикладной сервис и его контракты HTTP, аутентификации и интерфейса
пользователя остаются специфичными для продукта.

## Зависимости

Для RadixDB 1.2.4 добавьте SDK, асинхронный клиент и ORM в
доверенный сервис:

```toml
[dependencies]
radixdb-app-sdk = "1.2.4"
radixdb-client = { version = "1.2.4", features = ["tokio"] }
radixdb-orm = "1.2.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

При разработке из исходного дерева используйте `path`-зависимости, указывающие
на одну ревизию RadixDB. Не объединяйте сгенерированный код одного формата
descriptor с runtime-crates другого выпуска.

## Подключение к серверу

Прикладной сервис аутентифицируется как principal базы, выбирает базу и
оборачивает `AsyncConnection` в `AsyncConnectionTransport`:

```rust
use std::{sync::Arc, time::{Duration, Instant}};

use radixdb_app_sdk::{
    ApplicationClient, ApplicationSession, AsyncConnectionTransport,
    RequestContext, RequestId,
};
use radixdb_client::{AsyncConnection, AsyncTimeouts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = AsyncConnection::connect(
        "127.0.0.1:15432",
        AsyncTimeouts::default(),
    ).await?;
    connection
        .authenticate_database("inventory", "inventory_service", "secret")
        .await?;
    connection.select_database("inventory").await?;

    let transport = AsyncConnectionTransport::new(connection);
    let mut client = ApplicationClient::new(transport);

    let session = Arc::new(ApplicationSession::new(
        "user-42",
        "session-7",
        3,
        ["inventory.read".to_owned(), "inventory.write".to_owned()],
    )?);
    let context = RequestContext::new(RequestId::new("request-1001")?, session)
        .with_deadline(Instant::now() + Duration::from_secs(2));

    let descriptor = client.describe_database(&context, 8 * 1024 * 1024).await?;
    std::fs::create_dir_all("schema")?;
    std::fs::write("schema/database.json", descriptor.to_json()?)?;
    Ok(())
}
```

`authenticate_database` устанавливает principal RadixDB и его ACL базы.
`ApplicationSession` несёт уже проверенного пользователя продукта. Это разные
источники полномочий: создание прикладного permission не выдаёт привилегию
базы, а аутентификация в базе не доказывает личность пользователя браузера.

Создавайте `ApplicationSession` только из доверенного состояния
аутентификации. Никогда не копируйте subject ID, ревизию полномочий или набор
permissions непосредственно из тела запроса. Для каждого запроса используйте
новый `RequestId` и передавайте отмену прикладного сервера в
`CancellationSignal`.

## Получение схемы и генерация

Principal базы должен иметь явную привилегию `DESCRIBE`, чтобы получить
descriptor базы. Для одного описания схемы ему не требуется право чтения
таблиц:

```sql
GRANT DESCRIBE ON DATABASE inventory TO inventory_service;
```

Сгенерируйте прикладной исходный код из envelope descriptor:

```console
cargo run --package radixdb-app-sdk --bin radixdb-app-codegen -- \
  schema/database.json src/generated.rs
```

Перед записью исходного кода генератор проверяет fingerprints каждой таблицы,
процедуры и базы. Portable fingerprint не включает физические catalog IDs,
generations, timestamps и счётчики ревизий процедур. Поэтому две независимо
мигрированные базы создают одинаковый контракт, если их логические схемы
совпадают.

Сгенерированный модуль содержит:

- записи таблиц, типизированные столбцы, ключи и навигационные descriptors;
- типизированные структуры аргументов и результатов процедур;
- кодировщики параметров и декодировщики результатов;
- `DATABASE_SCHEMA_FINGERPRINT` для проверки совместимости при запуске;
- fingerprints отдельных процедур.

Храните descriptor и сгенерированный код в репозитории приложения. Повторяйте
генерацию в CI и отклоняйте неожиданный diff. При запуске получите актуальный
descriptor через `describe_database`, вычислите
`application_descriptor_fingerprints` и сравните его поле `schema` с
`generated::DATABASE_SCHEMA_FINGERPRINT` до начала приёма трафика.

## Вызов сгенерированной процедуры

Пусть база содержит процедуру `inventory.reserve_stock(product_id UUID,
quantity INTEGER)`. Сгенерированный модуль предоставляет
`InventoryReserveStockCall` с соответствующими Rust-полями:

```rust
use std::time::{Duration, Instant};

use radixdb_app_sdk::{IdempotencyKey, RequestContext, RequestId};
use crate::generated::InventoryReserveStockCall;

let context = RequestContext::new(RequestId::new("request-1002")?, session)
    .with_idempotency_key(IdempotencyKey::new("reserve-stock:order-91")?)
    .with_deadline(Instant::now() + Duration::from_secs(2));

let result = client.call(
    &context,
    InventoryReserveStockCall {
        product_id: "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e90".to_owned(),
        quantity: 4,
    },
).await?;
```

Идентификатор процедуры и позиционное кодирование поступают из
сгенерированного кода, а не из браузерного ввода. Декодировщик проверяет вид
результата, кардинальность, порядок столбцов, типы и допустимость NULL.
Несовпадение становится ошибкой контракта, а не частично разобранным
динамическим результатом.

Используйте типизированные процедуры для атомарных команд, а типизированные
ORM-запросы для чтения. Не публикуйте `ApplicationClient`, ORM IR или имена
сгенерированных процедур как универсальный HTTP endpoint.

## Контекст запроса

После создания `RequestContext` неизменяем и содержит:

- ограниченный непустой request ID;
- доверенный `ApplicationSession`;
- необязательный ключ идемпотентности;
- необязательный монотонный deadline;
- общий сигнал кооперативной отмены.

SDK проверяет отмену и deadline до запуска транспорта. Если одно из событий
происходит после начала выполнения, результат намеренно считается неизвестным:
база могла выполнить commit, хотя сервис не получил ответ.

## Повторные попытки и исход операции

Проверяйте обе характеристики `ApplicationError`:

```rust
use radixdb_app_sdk::{ApplicationError, OperationOutcome, RetryClass};

fn recovery_action(error: &ApplicationError) -> &'static str {
    match (error.retry_class(), error.outcome()) {
        (RetryClass::SafeAfterBackoff, OperationOutcome::RejectedBeforeCommit) => {
            "retry after bounded backoff"
        }
        (RetryClass::RequiresOutcomeResolution, OperationOutcome::Unknown) => {
            "reconcile by idempotency key before retrying"
        }
        _ => "return the classified error",
    }
}
```

Безопасной повторной попыткой считается только явно классифицированный сервером
backpressure. Потеря сети, ошибка протокола, timeout и отмена после запуска
транспорта требуют выяснения исхода. Проверяйте `client.is_reusable()` до
возврата транспорта в pool; отравленное или неопределённое соединение нужно
отбросить.

Ключ идемпотентности является метаданными контекста, а не автоматической
дедупликацией. Вызываемая процедура или прикладная схема должна сохранять и
контролировать соответствующий контракт дедупликации.

## Ограничение результатов

`TypedQuery::limits` и `TypedProcedure::limits` ограничивают число строк и
оценочный размер закодированных данных. Значения по умолчанию составляют 10 000
строк и 8 МиБ. Жёсткие пределы равны 100 000 строк и 64 МиБ. Перед возвратом
`ResultLimitExceeded` транспорт закрывает незавершённый cursor.

Уменьшайте лимиты для endpoints, которым нужна одна строка или небольшая
страница. Не используйте жёсткий максимум вместо прикладной пагинации.

## События

`ApplicationEvent` объявляет только сериализуемые topic и version:

```rust
use radixdb_app_sdk::ApplicationEvent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct StockReserved {
    product_id: String,
    quantity: i64,
}

impl ApplicationEvent for StockReserved {
    const TOPIC: &'static str = "inventory.stock-reserved";
    const VERSION: u32 = 1;
}
```

Trait не публикует событие. Добавляйте сериализованное событие в transactional
outbox в одной транзакции с прикладным изменением, после чего прикладной сервис
может преобразовать подтверждённые записи outbox в SSE, очередь или другую
систему доставки.

## Граница владения

`radixdb-app-sdk` владеет универсальными прикладными контрактами транспорта.
Сгенерированный SDK владеет одной схемой. Репозиторий продукта владеет
аутентификацией, авторизацией, маршрутами, скриптами, UI и бизнес-правилами.
RadixDB не зависит от продукта, а сгенерированный продуктовый код нельзя
копировать в репозиторий движка.

Смотрите также разделы [Rust TCP-клиент](../rust-client/),
[Rust ORM](../orm/), [управление доступом](../../administration/access-control/)
и [подпрограммы](../../programming/routines/).
