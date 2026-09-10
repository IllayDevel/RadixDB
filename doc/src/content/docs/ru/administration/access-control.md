---
title: Управление доступом
description: Principals, roles, ownership и object или column privileges в RadixDB 1.2.
---

Authorization RadixDB основана на catalog и deny-by-default. Каждый
non-bootstrap statement требует действующий session Principal, database
`CONNECT`, schema `USAGE` и privilege либо ownership для целевой операции.

Штатный TCP server связывает login/password authentication с теми же catalog
Principal IDs. Обычный TCP, опциональный TLS и loopback-only bootstrap recovery
описаны в [«Аутентификации»](../authentication/).

## Principals и roles

Principal является execution identity. Role объединяет privileges и может быть
выдана Principal или другой Role. Их имена используют одно normalized catalog
namespace.

```sql
CREATE PRINCIPAL alice;
CREATE ROLE report_reader;

GRANT CONNECT ON DATABASE application TO alice;
GRANT USAGE ON SCHEMA public TO alice;
GRANT SELECT (id, title) ON TABLE reports TO report_reader;
GRANT report_reader TO alice;
```

Membership рекурсивна и всегда активна; `SET ROLE` отсутствует. Cycles
отклоняются, а authorization раскрывает не более 65 536 subjects. `WITH ADMIN
OPTION` позволяет member выдавать Role дальше:

```sql
GRANT report_reader TO alice WITH ADMIN OPTION;
REVOKE ADMIN OPTION FOR report_reader FROM alice RESTRICT;
REVOKE report_reader FROM alice CASCADE;
```

Role grants сохраняют provenance grantor. `RESTRICT` является default и не даёт
удалить authority, на которой ещё держатся downstream memberships. Явный
`CASCADE` удаляет только зависимые paths этого grant; независимые paths
сохраняются. `REVOKE ADMIN OPTION FOR` снимает право делегирования, не удаляя
саму membership. Cycles отклоняются.

## Object privileges

| Target | Privileges |
| --- | --- |
| `DATABASE name` | `CONNECT` |
| `SCHEMA name` | `USAGE`, `CREATE` |
| `TABLE name` | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| `FUNCTION name(types)` | `EXECUTE` |
| `PROCEDURE name(types)` | `EXECUTE` |

Target `TABLE` также обозначает view для `SELECT`; отдельной формы `ON VIEW`
нет. Function и procedure targets используют input argument types для выбора
overload.

`SELECT`, `INSERT` и `UPDATE` принимают column lists. Table-level privilege
покрывает все columns. Column-only `SELECT` предназначен для однозначной
single-table projection; joins и subqueries с этой relation требуют table-level
`SELECT`. Predicates и expressions, читающие target columns, также требуют
`SELECT` независимо от write privileges.

```sql
GRANT INSERT (id, title), UPDATE (title) ON TABLE reports TO report_editor;
GRANT SELECT (id, title) ON TABLE reports TO report_reader WITH GRANT OPTION;
REVOKE GRANT OPTION FOR SELECT (id, title) ON TABLE reports
  FROM report_reader CASCADE;
```

`WITH GRANT OPTION` разрешает делегировать только выданный privilege и subset
columns. Grants сохраняют provenance grantor; `RESTRICT` защищает зависимые
grants, а явный `CASCADE` удаляет зависимые paths, не затрагивая независимые.

## Ownership и DDL

Bootstrap owner обходит обычные object checks. Owner объекта также имеет
неявные права на него. Creator становится owner таблицы, view, function или
procedure. Ownership transfer доступен только для tables, functions и
procedures, а target должен быть существующим Principal:

```sql
ALTER TABLE reports OWNER TO alice;
ALTER FUNCTION calculate_total(INTEGER) OWNER TO alice;
ALTER PROCEDURE refresh_reports() OWNER TO alice;
```

Только bootstrap может создавать schemas, Principals, Roles и Jobs. Schema
`USAGE` разрешает name resolution, а независимый schema privilege `CREATE` —
создание объектов. Ни одно из прав не подразумевает другое. Для создания index
или trigger дополнительно требуется ownership target table.

Principals и Roles поддерживают lifecycle со stable ID:

```sql
ALTER PRINCIPAL alice DISABLE;
ALTER PRINCIPAL alice ENABLE;
ALTER PRINCIPAL alice RENAME TO alice_archive;
ALTER ROLE report_reader RENAME TO report_viewer;
DROP PRINCIPAL alice_archive RESTRICT;
DROP ROLE report_viewer CASCADE;
```

`DROP` по умолчанию использует `RESTRICT`; явный `CASCADE` согласованно удаляет
catalog grants, memberships, ownership и Jobs `RUN AS`, не оставляя dangling
edges. Rename не меняет ObjectId, а удалённый ID не переиспользуется.

## Отзыв и выполнение statement

Authorization читает transaction-visible catalog на каждом statement entry.
Она применяется к prepared statements, cached plans, public ORM reads и
procedural SQL. Поэтому committed `REVOKE` действует на следующем выполнении без
ожидания refresh plan cache. Catalog changes GRANT и REVOKE атомарны.

Object grants хранятся отдельно по grantor, grantee и target. Owners и bootstrap
имеют implicit authority; другой subject делегирует только effective grant с
grant option. Grantor может отозвать только собственный provenance path.

## Отсутствующие policy-возможности

RadixDB 1.2 не реализует row-level security и `CREATE POLICY`. Также нет grants
для `PUBLIC`, default privileges, explicit deny, `SET ROLE` и public SQL-команды
для просмотра effective grants. Реализуйте row predicates в проверенных views
или application queries, но не называйте это RLS.

У routines есть дополнительная граница privilege elevation. Перед применением
`SECURITY DEFINER`, triggers или Jobs прочитайте
[«Безопасность routines»](../../programming/routine-security/).
