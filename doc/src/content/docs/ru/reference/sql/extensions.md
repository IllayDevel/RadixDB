---
title: Команды native extensions
description: Привязка trusted packages и создание их SQL types, functions, operators и planner support.
---

Эти команды связывают database catalog с package, уже допущенным сервером. SQL
не загружает shared library по пути и не скачивает код.

## Синтаксис

```text
CREATE EXTENSION [IF NOT EXISTS] extension_name VERSION 'exact_version';
DROP EXTENSION [IF EXISTS] extension_name RESTRICT;

CREATE TYPE schema.type_name
FROM EXTENSION extension_name AS 'local_id';
DROP TYPE [IF EXISTS] schema.type_name RESTRICT;

CREATE FUNCTION schema.function_name (
    [argument_name data_type [NULL | NOT NULL] [, ...]]
)
RETURNS data_type [NULL | NOT NULL]
LANGUAGE NATIVE
FROM EXTENSION extension_name AS 'local_id';
DROP FUNCTION schema.function_name (data_type [, ...]) RESTRICT;

CREATE OPERATOR schema.operator_symbol (
    [LEFTARG = data_type,]
    RIGHTARG = data_type,
    FUNCTION = schema.function_name(data_type [, ...])
)
FROM EXTENSION extension_name AS 'local_id';
DROP OPERATOR [IF EXISTS] schema.operator_symbol (
    [data_type], [data_type]
) RESTRICT;

CREATE OPERATOR CLASS schema.class_name
FOR TYPE data_type USING { BTREE | HASH | BITMAP | HNSW }
FROM EXTENSION extension_name AS 'local_id';
DROP OPERATOR CLASS [IF EXISTS] schema.class_name
USING { BTREE | HASH | BITMAP | HNSW } RESTRICT;

CREATE PLANNER SUPPORT schema.support_name
FOR FUNCTION schema.function_name(data_type [, ...])
FROM EXTENSION extension_name AS 'local_id';
DROP PLANNER SUPPORT [IF EXISTS] schema.support_name RESTRICT;
```

## Описание

`CREATE EXTENSION` записывает в database точную identity package. Имя и версия
должны совпадать с одной active entry startup registry. UUID, ABI range и
descriptor fingerprint сохраняются вместе с binding.

Остальные формы `CREATE` публикуют выбранные package descriptors как обычные
schema objects. `local_id` является стабильным export identifier, объявленным
автором extension; он чувствителен к регистру, содержит от 1 до 255 UTF-8 bytes
и не содержит NUL. Один package export можно связать только один раз. SQL names
могут отличаться от descriptor names, но переименование SQL object не меняет
его stable object identity.

Все declarations сверяются с package descriptor. SQL не может переопределить
type codec, volatility или strictness функции, operator signature, стратегии
operator class, key codec или поведение planner support.

## Привязка extension

Принимается только точный canonical SemVer без build metadata. Version range,
package path, URL, checksum override, `CASCADE`, `FORCE` и `IGNORE MISSING` не
входят в grammar 1.2. `IF NOT EXISTS` успешен только тогда, когда существующий
binding имеет тот же package UUID, version и fingerprint.

Registry выбирает одну active version для каждого package UUID при запуске.
Database остается привязанной к записанным version и fingerprint. В 1.2 нет
`ALTER EXTENSION UPDATE`.

## External types

Указанный descriptor должен быть external type. Stable object ID, codec
revision, semantic revision, storage shape, предел payload и callbacks
сравнения берутся из package. Значения сохраняют эту identity в catalog 6.2 и
protocol 17; они не взаимозаменяемы с `BYTES`.

Rust SDK 1.2 не публикует generic SQL text input/output callbacks. Создавайте
external values через native functions или plugin-aware protocol adapter, а не
через untyped SQL literal.

## Native functions

Типы arguments и result, nullability, strictness, volatility, parallel-safety,
cost, cancellation и batch capability должны в точности совпадать с
descriptor. Native aggregate, window и table-valued functions в 1.2 не
принимаются. Routines `LANGUAGE RADIX` являются отдельным механизмом.

## Operators и operator classes

Operator ссылается на уже связанную native function. Поддержаны prefix unary и
binary SQL forms; postfix-only operators отсутствуют. Закрытый operator
alphabet:

```text
= <> != < <= > >= + - * / % || & | ^ ~ << >> <=> && @> <@
```

Operator class связывает объявленные operators и canonical key encoder с
core-owned access method `BTREE`, `HASH`, `BITMAP` или `HNSW`. Strategy slots
определяет descriptor. B-tree classes требуют `<`, `<=`, `=`, `>=` и `>`;
hash и bitmap classes требуют `=`. Authoring SDK Rust 1.2 отклоняет external
HNSW classes, хотя SQL grammar резервирует этот method.

External operator class указывается в index definition так:

```sql
CREATE INDEX asset_point_idx
ON assets (position geo.point_btree) USING BTREE;
```

## Planner support

Planner support присоединяется к уже связанной native function. Он может
выдать bounded candidate ranges для объявленного operator class. Core planner
владеет scan и всегда применяет residual filtering, когда support descriptor
требует recheck. Extension не может предоставить executor node, обращаться к
storage напрямую или заменить cost-based planning.

## Порядок и транзакции

Referenced objects должны существовать в текущем transaction view. Создавайте
objects в порядке зависимостей, а удаляйте в обратном. Обычная установка
атомарна:

```sql
BEGIN;
CREATE EXTENSION radix_spatial VERSION '1.0.0';
CREATE TYPE geo.point FROM EXTENSION radix_spatial AS 'point';
CREATE FUNCTION geo.st_distance(left_point geo.point NOT NULL,
                                right_point geo.point NOT NULL)
RETURNS FLOAT NOT NULL
LANGUAGE NATIVE
FROM EXTENSION radix_spatial AS 'distance';
COMMIT;
```

Forward references завершаются ошибкой, а любой failed statement откатывает
catalog generation. `DROP EXTENSION ... RESTRICT` успешен только после явного
удаления всех dependent types, functions, operators, operator classes и
planner-support objects.

## Права

Database owner или `root` может создавать и удалять extension binding.
Dependent objects требуют ownership этого binding и `CREATE` на target schema;
`root` может выполнить операцию административно. Runtime calls функций и
operators требуют `EXECUTE` на backing function. Создание index также проходит
обычные проверки table rights и schema visibility.

Авторизация завершается до запуска native code. Plugin не получает principal,
ACL bypass или catalog-mutation handle.

## Ошибка после перезапуска

Если exact package или codec admission недоступны, database открывается в
restricted diagnostic mode. Восстановите совпадающий package и перезапустите
server либо удалите binding от имени `root` командой
`DROP EXTENSION ... RESTRICT` после удаления dependents. DDL не может обойти
эту проверку.

## См. также

См. [Установка и эксплуатация extensions](../../../administration/extensions/),
[Разработка native extensions](../../../programming/native-extensions/),
[CREATE INDEX](../create-index/) и
[`cargo radixdb-plugin`](../../programs/cargo-radixdb-plugin/).
