---
title: Transactions and Concurrency
description: Atomic changes, MVCC visibility, isolation levels, savepoints and conflict handling.
---

A transaction groups statements into one atomic change. RadixDB uses multiversion
concurrency control (MVCC): readers choose visible committed row versions while a
writer keeps its uncommitted versions private. This chapter describes the actual
1.2 contract, including the differences between `READ COMMITTED` and `SNAPSHOT`.

## Autocommit and explicit transactions

Without `BEGIN`, every statement is its own transaction. Use an explicit
transaction when several statements must either all become visible or all be
discarded. The following transfer preserves the total balance of 160.

```sql
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    balance INTEGER NOT NULL
);
INSERT INTO accounts VALUES (1, 100), (2, 60);

BEGIN;
UPDATE accounts SET balance = balance - 25 WHERE id = 1;
UPDATE accounts SET balance = balance + 25 WHERE id = 2;
SELECT id, balance FROM accounts ORDER BY id;
COMMIT;

SELECT SUM(balance) AS total_balance FROM accounts;
```

The transaction reads its own writes: the SELECT before `COMMIT` returns balances
75 and 85. Other connections cannot see either update until the commit succeeds.
After `COMMIT`, both updates become visible together.

## Rollback

`ROLLBACK` discards every change made since `BEGIN`. It also ends the transaction.

```sql
BEGIN;
UPDATE accounts SET balance = 0 WHERE id = 1;
ROLLBACK;

SELECT id, balance FROM accounts ORDER BY id;
```

The balances remain 75 and 85. Dropping an embedded `Transaction` handle without
committing also rolls it back, but application code should still finish every
transaction explicitly.

## Savepoints

A savepoint marks a position inside the current transaction. Rolling back to it
undoes later work without discarding earlier work. The target savepoint remains
available until it is released or the transaction ends.

```sql
BEGIN;
UPDATE accounts SET balance = 90 WHERE id = 1;
SAVEPOINT before_fee;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_fee;
RELEASE SAVEPOINT before_fee;
COMMIT;

SELECT balance FROM accounts WHERE id = 1;
```

The final balance is 90. Unquoted savepoint names are case-insensitive. A network
client uses its dedicated `savepoint`, `rollback_to_savepoint` and
`release_savepoint` methods; see [Client Interfaces](../../clients/overview/).

## MVCC visibility

An uncommitted insert, update or delete is visible to the transaction that made
it and invisible to other transactions. A rollback removes the private version;
a commit publishes the transaction as one visibility boundary. Readers do not
observe dirty data.

For each isolation example below, start with a new database and prepare this
table before opening the two connections:

```sql
CREATE TABLE isolation_demo (
    id INTEGER PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT INTO isolation_demo VALUES (1, 10), (2, 20);
```

MVCC visibility does not remove write conflicts. It lets readers continue from
an appropriate committed version while the engine coordinates concurrent
writers separately.

## Read committed

`READ COMMITTED` is the default. Every statement sees data committed before that
statement begins, plus the transaction's own writes. Two reads in one transaction
may therefore observe different committed states.

```sql
-- Connection A
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT SUM(value) FROM isolation_demo; -- 30

-- Connection B
BEGIN;
UPDATE isolation_demo SET value = value + 100;
COMMIT;

-- Connection A
SELECT SUM(value) FROM isolation_demo; -- 230
ROLLBACK;
```

Connection A does not see Connection B's uncommitted changes. Its second SELECT
runs after B commits and advances to the new committed state.

## Snapshot isolation

`SNAPSHOT` fixes the committed view at the beginning of the transaction. Later
commits by other connections remain outside that view, while the transaction
still sees its own writes.

```sql
-- Connection A
BEGIN ISOLATION LEVEL SNAPSHOT;
SELECT SUM(value) FROM isolation_demo; -- 30

-- Connection B
BEGIN;
UPDATE isolation_demo SET value = value + 100;
COMMIT;

-- Connection A
SELECT SUM(value) FROM isolation_demo; -- still 30
ROLLBACK;

-- Connection A, after ending the snapshot
SELECT SUM(value) FROM isolation_demo; -- 230
```

Snapshot isolation is not serializable isolation. It provides a stable read view
and detects applicable write conflicts, but applications must not assume that it
prevents every anomaly involving writes to different rows.

## Competing writers and retries

Writers claiming the same row or unique key wait for the current owner. The
production wait budget is 10 seconds for one unchanged owner. After the owner
commits or rolls back, a waiting update rechecks the current committed row and
its `WHERE` condition before applying its change.

A cycle in the engine-wide wait graph fails one participant with a serialization
conflict. An unchanged blocker that outlives the budget produces a row-lock
timeout. Both are retryable transaction outcomes: roll back the entire transaction,
apply backoff, and retry it from `BEGIN`. Do not retry only the failed statement.

Constraint failures are also explicit errors. A network transaction remains
rollback-capable after a rejected statement. Check the client transaction state,
then issue `ROLLBACK` before reusing the connection unless the operation's API
explicitly reports that it already ended the transaction.

```sql
BEGIN;
INSERT INTO accounts VALUES (3, 10);
INSERT INTO accounts VALUES (3, 20); -- primary-key error
ROLLBACK;
```

The first insert is not published. DDL and DML may share an explicit transaction;
catalog changes owned by that transaction remain private until commit.

## Supported isolation boundary

The 1.2 SQL executor accepts only `READ COMMITTED` and `SNAPSHOT`. The following
levels are rejected rather than silently treated as a weaker level:

```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
BEGIN ISOLATION LEVEL REPEATABLE READ;
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
```

`SET ISOLATIONLEVEL`, `SET ISOLATION_LEVEL` and `SET TRANSACTION_ISOLATION`
change only the current connection's default for future transactions. They are
rejected inside an active transaction because its isolation was fixed by
`BEGIN`. `SHOW ISOLATION_LEVEL` returns the effective connection-local default;
another handle that shares the engine is unaffected.

## Client boundaries in 1.2

The embedded Rust API selects isolation with `begin_with_isolation`. The TCP
client sends a dedicated transaction message through the method of the same name;
generic wire `execute` deliberately rejects transaction-control SQL. Both APIs
also expose dedicated commit, rollback and savepoint methods.

The 1.2 CLI preserves `READ COMMITTED` and `SNAPSHOT` clauses on `BEGIN` and
routes `SAVEPOINT`, `ROLLBACK TO` and `RELEASE` through its active transaction
handle. Unsupported levels fail closed. If a multi-statement batch fails, the
CLI rolls back and does not publish a successful prefix.

Transaction isolation and durability are different contracts. `COMMIT` publishes
one atomic transaction; the configured synchronization mode determines when its
storage writes are forced to durable media. The administration chapters specify
those persistence settings.
