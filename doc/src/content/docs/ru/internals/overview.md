---
title: Обзор архитектуры
description: Владельцы crates, путь запроса и время жизни состояния в RadixDB 1.2.
---

Эта глава представляет карту реализованной архитектуры RadixDB 1.2. Она
объясняет путь запроса и владельцев состояния, но не вводит второй Rust,
storage или wire contract.

## Канонические контракты

| Предмет | Канонический источник |
| --- | --- |
| Поддерживаемый application API | Top-level exports packages `radixdb` и `radixdb-client` |
| Владение crates | Workspace manifests, public modules crates и исполняемые boundary tests |
| Physical format и recovery | Codecs, format constants и recovery tests в `radixdb-storage` |
| Wire format | `crates/radixdb-protocol/src/lib.rs` |
| ABI native extensions и package admission | `radixdb-plugin-abi`, `radixdb-plugin-host` и public SDK `radixdb-plugin` |
| Поведение SQL | Parser, executor и versioned SQL coverage matrix |

Internal modules могут меняться без source compatibility. Приложения должны
использовать интерфейсы из раздела [«Клиентские интерфейсы»](../../clients/overview/).

## Граф crates

Implementation dependencies направлены от composition layers к более узким
владельцам:

```text
radixdb-core ---> radixdb-sql ---------+
      |                                |
      +--------> radixdb-functions ----+--> radixdb-executor --+
      |                                |                       |
      +--------> radixdb-storage ------+                       v
                                                    radixdb-api --> radixdb
radixdb-orm ---------------------------------------------^
radixdb-protocol ---> radixdb-client
        |
        +---------------------------------------> server runtime
```

`radixdb-core` владеет нейтральными values, schemas и errors. `radixdb-sql`
владеет lexer, AST и parser. `radixdb-storage` владеет MVCC, WAL, indexes и
physical generations без зависимости от SQL. `radixdb-executor` связывает и
планирует SQL, проверяет права и исполняет relational operators. `radixdb-api`
собирает этих владельцев за embedded handles. Корневой package `radixdb`
является public facade и содержит runtime серверного process.

`radixdb-protocol` намеренно нейтрален: server и `radixdb-client` используют
одни message и codec types. Client не компонует движок. Исторические aliases
корневых modules сохраняют source compatibility, но не являются отдельными
владельцами реализации или extension API.

## Владение crates

| Crate | Ответственность |
| --- | --- |
| `radixdb` | Public embedded facade, CLI и композиция server process |
| `radixdb-core` | Нейтральные values, schemas и errors |
| `radixdb-sql` | SQL lexer, AST и parser |
| `radixdb-functions` | Встроенные scalar, aggregate и semantic functions |
| `radixdb-storage` | MVCC, WAL, indexes, physical generations и recovery |
| `radixdb-executor` | Authorization, binding, planning и relational execution |
| `radixdb-api` | Embedded database и connection handles |
| `radixdb-protocol` | Versioned wire messages и codecs |
| `radixdb-client` | Синхронный и асинхронный TCP client |
| `radixdb-orm` | Language-neutral ORM IR, schema descriptors и SQL rendering |
| `radixdb-procedural` | Bounded procedural bytecode и runtime contracts |
| `radixdb-plugin-abi` | Stable types и constants ABI native extensions |
| `radixdb-plugin-host` | Admission, loading и invocation native packages |
| `radixdb-plugin` | Безопасный extension SDK |
| `radixdb-plugin-macros` | Генерация extension descriptor и callbacks |
| `cargo-radixdb-plugin` | Команды создания, проверки и упаковки extensions |

Application surfaces являются только top-level `radixdb`, `radixdb-client`,
`radixdb-orm` и документированные интерфейсы extension SDK. Implementation
crates являются workspace owners, а не отдельными compatibility promises.

## Путь native extension

Native packages входят через отдельный fail-closed composition path:

```text
plugin cdylib -> C ABI 1.0 descriptor -> startup package admission
                                         |
                                         v
immutable process registry -> exact catalog binding -> executor callback
             |                        |                 |
             |                        |                 +-> typed scalar/batch
             |                        +-> type/function/operator identities
             +-> no storage, WAL, catalog or ACL handles
```

`radixdb-plugin-abi` владеет numeric values и C layouts.
`radixdb-plugin-host` владеет admission manifest, platform, checksum,
ownership, descriptor и callbacks. Безопасный SDK `radixdb-plugin` и
`radixdb-plugin-macros` генерируют этот ABI из bounded Rust declarations;
`cargo-radixdb-plugin` владеет repeatable authoring и packaging workflow.

Server создает один immutable registry до bind listener и удерживает loaded
libraries до завершения process. Catalog 6.2 хранит exact package,
fingerprint, object и codec identities. Executor разрешает native functions,
operators, operator classes и planner support через этот registry, но
authorization и storage access остаются core-owned. Plugin storage engine,
catalog mutation callback, hot unload и hidden network install отсутствуют.
См. [«Разработку native extensions»](../../programming/native-extensions/).

## Путь запроса

Embedded и TCP requests сходятся до исполнения SQL:

```text
embedded Database API ------------------------------+
                                                    v
TCP frame -> session state -> selected Database -> Executor
                                                    |
                  parse/cache -> authorize -> bind/plan -> operators
                                                    |
                         MVCC transaction -> WAL -> visibility
                                                    |
                  QueryResult -> cursor/batches -> caller or TCP frame
```

Для обычного SQL executor сначала пробует подходящий cached или
borrowed-parameter fast path. Иначе он разбирает program, допускает один или
несколько statements, проверяет права, связывает navigable references,
устанавливает statement visibility boundary и направляет DDL, DML, SELECT или
utility work конкретному владельцу. Для SELECT planner выбирает storage access
и join operators; result pipeline применяет projection, filtering,
aggregation, windows, ordering, set operations и paging по требованиям query.

Mutations входят через ту же границу executor, но используют MVCC transaction.
Storage engine владеет WAL durability и точкой commit visibility.
Statement-level savepoint не позволяет неуспешному statement внутри explicit
transaction оставить частичный эффект.

## Владение состоянием

| Время жизни | Владелец состояния | Что разделяет состояние |
| --- | --- | --- |
| Process | Server config, immutable plugin registry, listener, admission connections, global frame budget и cancellation registry | Все server sessions |
| Database root | `DatabaseOwner`: один `MVCCEngine`, writer lock, semantic cache и feedback cache на canonical DSN | Connections к тому же DSN |
| Connection | Свои `DatabaseInner`, `Executor`, parsed-plan cache и скрытая SQL transaction | Clone получает нового connection-local owner |
| Statement | `ExecutionContext`, параметры, cancellation, statement scope, fences и savepoint | Только nested work этого statement |
| Cursor | Result iterator и pending row/column batch | Один active cursor в TCP session |

Глобальный embedded registry сериализует первое открытие canonical DSN и
разделяет полученный durable engine. Он не разделяет executor или SQL
transaction. Поэтому `Database::clone()` видит те же committed data, но не
наследует active transaction другого handle.

Server имеет второй registry имён под настроенным data root. Он хранит состояния
opening, ready и retryable failed и выдаёт connection handle от embedded owner.
Recovery разных database names может идти независимо; у одного имени всегда
один opening owner.

## Границы конкурентного доступа

Storage engine публикует lifecycle state до допуска transactions. Catalog DDL,
statement visibility, sealing, snapshots и maintenance используют отдельные
fences с явным порядком захвата. Readers удерживают immutable snapshots catalog
и physical generation; publisher строит successor до замены общей generation.

Эти fences являются внутренней координацией, а не public locking API. Гарантии
для приложения описаны в главе [«Транзакции»](../../sql/transactions/), а
physical publication и recovery в [«Внутреннем устройстве хранения»](../storage/).
