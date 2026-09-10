---
title: Rust ORM
description: Dynamic и generated records без сокрытия SQL, транзакций и ссылок.
---

`radixdb-orm` определяет независимые от transport schema descriptors, typed
values, records, query builders и версионированное промежуточное представление.
Выполнение предоставляет встраиваемый API или `radixdb-client`; выбор ORM не
выбирает режим развёртывания и не создаёт отдельное соединение.

## Builders и выполнение

Каждый builder формирует `IrDocument`. `OrmBuilder::to_json()` выдаёт
версионированное представление `radixdb.orm.v1`, а `to_sql()` возвращает SQL с
typed parameters. Значения передаются параметрами, не интерполируются в текст;
в IR нет raw-SQL expression node.

```rust
use radixdb_orm::{table, Expr, OrmBuilder, QueryBuilder};

let query = QueryBuilder::from_relation(table("tasks"))
    .select([Expr::column("id"), Expr::column("title")])
    .filter(Expr::column("done").eq(false));
let compiled = query.to_sql()?;
println!("{} {:?}", compiled.sql, compiled.parameters);
```

`query.fetch(&mut connection)` выполняется на заимствованном TCP-соединении.
Встраиваемое приложение передаёт `&db` или `&mut transaction`. Builders не
владеют сессией и не могут незаметно выйти за границы транзакции вызывающего кода.

## Dynamic entities и records

`connection.entity("tasks")` читает live descriptor таблицы
`radixdb.schema.v1` и создаёт `DynamicEntity`. При построении builder его столбцы
проверяются по этому descriptor.

```rust
use radixdb_orm::{DynamicRecord, TypedValue};

let tasks = connection.entity("tasks")?;
let mut task = DynamicRecord::new(tasks.descriptor().clone());
task.set("id", TypedValue::Integer(10))?;
task.set("title", TypedValue::Text("write documentation".to_string()))?;
task.insert(&mut connection)?;

task.set("done", TypedValue::Boolean(true))?;
task.update(&mut connection)?;
assert!(!task.is_dirty());
```

`insert()`, `save()` и `update()` используют `RETURNING *` и гидратируют record
только после полного успешного результата. При ошибке dirty fields сохраняются.
`save()` является upsert по primary key, а не сохранением графа. `delete()`
возвращает ошибку для отсутствующей записи. `set_null()` записывает typed NULL,
а `unset()` исключает поле из следующей mutation.

## Ссылки

`Reference<T>` в generated code и runtime `DynamicReference` содержат только
проверенный target key. Они не хранят и не загружают target row. Dynamic links
требуют одно-столбцовый primary key либо `UNIQUE NOT NULL` key и проверяют, что
source column имеет соответствующий foreign key:

```rust
let owners = connection.entity("owners")?;
let tasks = connection.entity("tasks")?;
let owner = owners.reference("id", TypedValue::Integer(1))?;

let mut task = DynamicRecord::new(tasks.descriptor().clone());
task.set("id", TypedValue::Integer(10))?;
task.set_reference("owner_id", &owner)?;
```

Навигационные SQL paths являются read-only query expressions. Присваивание
ссылки по-прежнему явно записывает исходный foreign-key field.

## Generated models

Generated code начинается с явного экспорта live descriptor:

```sql
DESCRIBE DATABASE FORMAT JSON
```

Запустите детерминированную генерацию offline и храните проверенный schema
descriptor и generated source в VCS приложения:

```bash
cargo run --locked -p radixdb-orm --bin radixdb-orm-codegen -- \
  schema.json src/generated_schema.rs
```

Генерация не выполняется procedural macro или `build.rs` и не подключается к
базе. Generated entities предоставляют typed columns, typed records,
key/reference constructors и CRUD methods. Они содержат schema fingerprint;
несовпадение с live descriptor закрывается ошибкой `SchemaChanged`. После
принятой schema migration экспортируйте descriptor заново, перегенерируйте код
и проверьте diff.

## Одна транзакция SQL и ORM

Raw SQL и ORM используют одну сессию, которой владеет вызывающий код. TCP
transaction начинается и заканчивается специальными methods:

```rust
connection.begin()?;
connection.execute(
    "INSERT INTO tasks (id, title) VALUES (11, 'raw row')",
)?;
let rows = tasks
    .query()
    .select([tasks.column("id")?.expr()])
    .fetch(&mut connection)?;
connection.rollback()?;
```

ORM query видит raw uncommitted write, потому что оба заимствуют одно соединение.
Во встраиваемом варианте вызовите `db.begin()` и передавайте `&mut transaction`.
После outer rollback отбросьте или перечитайте in-memory records, которые были
гидратированы внутри транзакции: record objects не откатывают локальные поля.

## Границы автоматизации

ORM 1.2 намеренно не предоставляет identity map, lazy loading, automatic
relationship fetch, cascade save/delete, reverse collections, automatic schema
diff или migration. References являются одно-столбцовыми keys. Приложение
управляет границами транзакций, batching, retries и сверкой после неопределённого
TCP outcome.

Полная программа `doc/examples/clients/orm.rs` проверяет привязку live
descriptor, валидированную ссылку, dynamic CRUD и общий rollback raw SQL/ORM на
одном соединении. Более крупное приложение `examples/public/rust-orm` показывает
offline workflow generated models. Путь `orm_quickstart.rs --transaction-smoke`
исполняет raw write -> ORM read -> rollback, commit и восстановление после
ошибки на одном connection через специальные transaction methods.

Вернитесь к [обзору клиентских интерфейсов](../overview/), чтобы сравнить режимы
развёртывания.
