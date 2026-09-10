---
title: Conventions
description: Reading commands, SQL examples and version notices.
---

Shell commands and SQL statements are shown in separate code blocks. Run shell
commands in a terminal; enter SQL in the database client. The prompts are omitted
so that they do not become part of copied commands.

SQL keywords are written in uppercase and example identifiers in lowercase.
A semicolon ends a statement. Quoted text values such as `'Alice'` are data,
not identifiers. SQL `NULL` denotes a missing value, not an empty string.

## Examples and Results

The English and Russian editions use the same identifiers, fixture data and
executable SQL files. Explanations are translated; commands are not.
Result tables show the expected values rather than terminal border characters.
An explicit `ORDER BY` is used where row order matters.

Shell variables such as `tutorial_dir` must remain available for the exercise.
Do not substitute a production database path into a tutorial command.
A database used by one exercise is not a backup of another database.

## Versions and Limits

The heading identifies the documentation target. Build details distinguish
that target from the application manifest and the source revision. An explicit
limitation means the supported 1.2 behavior is narrower than the surrounding
feature; behavior described only in a plan is not part of the user contract.

Limits are qualified as implementation limits, configuration limits or measured
workload sizes. Measurements are not universal capacity guarantees.
