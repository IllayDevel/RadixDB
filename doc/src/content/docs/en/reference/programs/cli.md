---
title: radixdb-cli
description: Command-line reference for local SQL, maintenance and logical migration.
---

`radixdb-cli` opens `memory://` and `file://` databases directly. It is not a
TCP client for `radixdb-server`. Run `radixdb-cli --version` to record the exact
binary identity and `radixdb-cli --help` for its built-in summary.

## Synopsis

```text
radixdb-cli [OPTIONS]
```

With no action option, the program starts an interactive SQL session. Exactly
one action from `--execute`, `--file`, `--restore`, `--snapshot`,
`--reset-storage`, `--export-sql` and `--import-sql` may be selected.

## Connection and output

| Option | Default | Meaning |
| --- | --- | --- |
| `-d`, `--db DSN` | `memory://` | Embedded database DSN |
| `-j`, `--json` | off | Machine-readable result objects on stdout |
| `-q`, `--quiet` | off | Suppress connection and persistence messages |
| `-l`, `--limit ROWS` | `40` | Retained display rows; `0` uses a 100,000-row safety cap |
| `-t`, `--timeout MS` | `0` | Statement timeout; `0` disables it |
| `-h`, `--help` | | Print help |
| `-V`, `--version` | | Print version and build identity |

JSON result objects report total, retained and omitted rows and whether the
head/tail window was truncated. A single accumulated CLI statement is limited
to 16 MiB. `--quiet --json` is the stable combination for scripts.

## SQL actions

| Option | Input | Result |
| --- | --- | --- |
| `-e`, `--execute SQL` | Exactly one statement | Execute and exit |
| `-f`, `--file PATH` | UTF-8 SQL, at most 16 MiB | Execute statements in order and exit |
| no action | Terminal input | Interactive session with history |

```sh
radixdb-cli -q -j -d memory:// --execute "SELECT 42 AS answer"
radixdb-cli -d file:///srv/radixdb/local --file schema.sql
```

Interactive commands are `help`, `\h` or `\?` for help and `exit`, `quit` or
`\q` to leave. A trailing SQL semicolon completes multi-line input. History is
stored in `~/.radixdb_history`; an active transaction is rolled back on exit.

The CLI preserves `BEGIN ISOLATION LEVEL READ COMMITTED|SNAPSHOT` and routes
`SAVEPOINT`, `ROLLBACK TO` and `RELEASE` through the active embedded
transaction. Unsupported isolation levels fail closed; a failed batch rolls
back without publishing its successful prefix.

## Persistence overrides

These options build overrides for a `file://` DSN:

| Option | Unit or values | Default |
| --- | --- | --- |
| `-p`, `--profile PROFILE` | `fast`, `normal`, `durable` | none |
| `-s`, `--sync MODE` | `none`, `normal`, `full` | DSN/default |
| `--checkpoint-interval SECONDS` | seconds, `u32` | `60` |
| `--compact-threshold COUNT` | volumes, `u32` | `4` |
| `--volume-cache-size MB` | MiB, `u32` | `1024` |
| `--wal-max-size MB` | MiB, `u32` | `64` |
| `--compression on\|off` | Boolean | `on` |
| `--keep-snapshots COUNT` | database snapshots, `u32` | `3` |
| `--no-checkpoint-on-close` | flag | checkpoint enabled |

`--sync` and profiles emit the canonical `sync_mode` key. Explicit CLI options
override the matching DSN value, while an explicit profile supplies its complete
persistence preset before individual flags are applied. The banner reads the
effective opened database configuration, so `none`, `normal` and `full` match
runtime persistence rather than a second CLI-side calculation.

The [configuration index](../../configuration/) lists every accepted file DSN
key. CLI options override or append only their corresponding subset; unknown
and duplicate DSN keys fail closed.

## Snapshot and reset actions

| Option | Scope | Behavior |
| --- | --- | --- |
| `--snapshot` | `file://` | Create one complete physical database snapshot |
| `--restore [SNAPSHOT_ID]` | `file://` | Restore latest or selected 32-hex snapshot atomically |
| `--reset-storage` | `file://` | Destructively replace the complete storage generation |

```sh
radixdb-cli -d file:///srv/radixdb/local --snapshot
radixdb-cli -d file:///srv/radixdb/local --restore 0123456789abcdef0123456789abcdef
```

`--snapshot` prints the committed 32-hex snapshot ID accepted by `--restore`;
retention is database-wide. Never run restore or reset while another process
owns the database, and keep an external backup before either destructive
operation.

## Logical migration

| Option | Requirement |
| --- | --- |
| `--export-sql PATH\|-` | Source database must be readable by this binary |
| `--import-sql PATH\|-` | Target must be a nonexistent `file://` database |

Export produces a checksummed, versioned UTF-8 SQL dump from one consistent
snapshot. Import builds and closes a full-sync sibling staging database before
atomically publishing the target. `-` selects stdout or stdin, but a file is the
preferred retained cutover artifact.

Every non-interactive failure returns a non-zero process status and writes its
diagnostic to stderr. See [backup and restore](../../../administration/backup-restore/)
and [upgrading](../../../administration/upgrading/) for operational procedures.
