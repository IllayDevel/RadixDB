import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION;
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);

function blocks(locale) {
  const source = readFileSync(new URL(`../src/content/docs/${locale}/sql/queries.md`, import.meta.url), 'utf8');
  return [...source.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}
function values(output) {
  return output.filter(item => Array.isArray(item.rows)).map(result => {
    assert.equal(result.truncated, false);
    assert.equal(result.count, result.rows.length);
    return result.rows.map(row => row.map(cell => cell.value));
  });
}
function execute(database, sql, diagnostic) {
  const result = spawnSync(cli, ['-q', '-j', '-d', `file://${database}?sync_mode=full`, `--execute=${sql}`], {
    encoding: 'utf8', timeout: 30000,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null);
  if (diagnostic) {
    assert.notEqual(result.status, 0, `Expected rejection: ${sql}`);
    assert(result.stderr.toLowerCase().includes(diagnostic), result.stderr);
    return [];
  }
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

const sql = blocks('en');
assert.deepEqual(sql, blocks('ru'));
assert.equal(sql.length, 13);
const root = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-queries-'));
const database = path.join(root, 'database');

try {
  assert.deepEqual(execute(database, sql[0]), [0, 0, 0, 3, 4, 3].map(rows_affected => ({ rows_affected })));
  assert.deepEqual(values(execute(database, sql[1])), [[
    ['2', 'Boris', '90'],
    ['3', 'Clara', '80'],
  ]]);

  const joins = values(execute(database, sql[2]));
  assert.equal(joins.length, 5);
  assert.deepEqual(joins[0], [
    ['Alice', 'Engineering'], ['Boris', 'Engineering'], ['Clara', 'Support'],
  ]);
  assert.deepEqual(joins[1].at(-1), ['Dan', null]);
  assert.deepEqual(joins[2].at(-1), ['3', 'Sales', null]);
  assert.deepEqual(joins[3].slice(-2), [
    ['4', 'Dan', null, null], [null, null, '3', 'Sales'],
  ]);
  assert.deepEqual(joins[4], [
    ['Alice', 'Engineering'], ['Alice', 'Sales'], ['Alice', 'Support'],
  ]);

  assert.deepEqual(values(execute(database, sql[3])), [[
    ['Engineering', '2', '210', '105'],
    ['Support', '1', '80', '80'],
  ]]);
  assert.deepEqual(values(execute(database, sql[4])), [
    [['Alice', '120']],
    [['Alice'], ['Clara']],
    [['Alice']],
  ]);
  assert.deepEqual(values(execute(database, sql[5])), [[
    ['Alice', 'Engineering'], ['Boris', 'Engineering'], ['Clara', 'Support'],
  ]]);
  assert.deepEqual(values(execute(database, sql[6])), [[
    ['Engineering', '210'], ['Support', '80'],
  ]]);
  assert.deepEqual(values(execute(database, sql[7])), [[['1'], ['2'], ['3'], ['4']]]);
  assert.deepEqual(values(execute(database, sql[8])), [[
    ['Alice', '1', '120', '1', '210'],
    ['Boris', '1', '90', '2', '210'],
    ['Clara', '2', '80', '1', '80'],
    ['Dan', null, '70', '1', '70'],
  ]]);

  for (const [index, diagnostic] of [
    [9, 'only supports union all'],
    [10, 'expected expression'],
    [11, "expected ';' between statements"],
    [12, 'failed against output columns'],
  ]) execute(database, sql[index], diagnostic);

  const natural = execute(database, `
    SELECT e.name AS employee, d.department_label
    FROM employees AS e
    NATURAL JOIN (
        SELECT id AS department_id, name AS department_label FROM departments
    ) AS d
    ORDER BY e.id;
  `);
  assert.deepEqual(values(natural), [[
    ['Alice', 'Engineering'], ['Boris', 'Engineering'], ['Clara', 'Support'],
  ]]);

  const windowRegistry = execute(database, `
    SELECT name,
           RANK() OVER (ORDER BY salary DESC) AS rank_value,
           DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_value,
           NTILE(2) OVER (ORDER BY salary DESC) AS bucket,
           LAG(name) OVER (ORDER BY salary DESC) AS previous_name,
           LEAD(name) OVER (ORDER BY salary DESC) AS next_name,
           FIRST_VALUE(name) OVER (
               ORDER BY salary DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS first_name,
           LAST_VALUE(name) OVER (
               ORDER BY salary DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS last_name
    FROM employees
    ORDER BY salary DESC;
  `);
  const windowRows = values(windowRegistry)[0];
  assert.deepEqual(windowRows[0], ['Alice', '1', '1', '1', null, 'Boris', 'Alice', 'Dan']);
  assert.deepEqual(windowRows.at(-1), ['Dan', '4', '4', '2', 'Clara', null, 'Alice', 'Dan']);

  execute(database, `
    CREATE TABLE nullable_windows (id INTEGER PRIMARY KEY, bucket INTEGER, score INTEGER);
    CREATE INDEX nullable_windows_bucket ON nullable_windows(bucket);
    INSERT INTO nullable_windows VALUES (1, NULL, 30), (2, NULL, 20), (3, 7, 10);
    PRAGMA CHECKPOINT;
  `);
  const nullablePartition = values(execute(database, `
    SELECT id, bucket,
           ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY score DESC) AS position
    FROM nullable_windows
    ORDER BY id;
  `));
  assert.deepEqual(nullablePartition, [[
    ['1', null, '1'], ['2', null, '2'], ['3', '7', '1'],
  ]]);

  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    published_blocks: sql.length,
    successful_blocks: 9,
    expected_errors: 4,
    join_forms: 7,
    window_functions: 10,
    nullable_indexed_window_partition: 'verified-after-reopen',
    passed: true,
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
