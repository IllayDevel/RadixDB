import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to the pinned binary');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);

function blocks(locale, chapter) {
  const source = readFileSync(new URL(`../src/content/docs/${locale}/sql/${chapter}.md`, import.meta.url), 'utf8');
  return [...source.matchAll(/^```sql\n([\s\S]*?)^```/gm)].map(match => match[1]);
}
function queryResults(output) {
  return output.filter(item => Array.isArray(item.rows));
}
function cellValue(cell) {
  return cell.value;
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

const root = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-schema-'));
const ddl = blocks('en', 'ddl');
const allIndexes = blocks('en', 'indexes');
assert.deepEqual(ddl, blocks('ru', 'ddl'));
assert.deepEqual(allIndexes, blocks('ru', 'indexes'));
assert.equal(ddl.length, 7);
assert.equal(allIndexes.length, 10);
const indexes = allIndexes.filter(block => !block.includes(' geo.point_btree'));
assert.equal(indexes.length, 9);

try {
  const ddlDatabase = path.join(root, 'ddl');
  let output = execute(ddlDatabase, ddl[0]);
  let queries = queryResults(output);
  assert.deepEqual(output.slice(0, 4), [0, 0, 1, 1].map(rows_affected => ({ rows_affected })));
  assert.equal(queries[0].count, 5);
  assert.deepEqual(queries[0].columns, ['Field', 'Type', 'Null', 'Key', 'Default', 'Extra']);
  assert.deepEqual(queries[1].rows[0].map(cellValue), ['1', '1', 'A-1', '0', null]);
  for (const [index, diagnostic] of ['not null', 'check constraint', 'foreign key', 'unique constraint'].entries()) {
    execute(ddlDatabase, ddl[index + 1], diagnostic);
  }
  output = execute(ddlDatabase, ddl[5]);
  queries = queryResults(output);
  assert.deepEqual(queries[0].rows[0].map(cellValue), ['1', 'one', 'warehouse']);
  assert.equal(queries[1].count, 2);
  assert.deepEqual(queries[1].rows[1].map(cellValue), ['title', 'TEXT', 'NO', '', "'untitled'", '']);
  assert.deepEqual(queries[2].rows[0].map(cellValue), ['1', 'one']);
  output = execute(ddlDatabase, ddl[6]);
  queries = queryResults(output);
  assert.equal(queries.length, 1);
  assert.equal(queries[0].count, 0);
  assert.deepEqual(queries[0].columns, ['table_name']);

  const indexDatabase = path.join(root, 'indexes');
  output = execute(indexDatabase, indexes[0]);
  queries = queryResults(output);
  const shown = new Map(queries[0].rows.map(row => [cellValue(row[1]), row.map(cellValue)]));
  assert.equal(shown.size, 4);
  assert.deepEqual(shown.get('contacts_active_idx').slice(2, 5), ['active', 'BITMAP', false]);
  assert.deepEqual(shown.get('contacts_email_active_uidx').slice(2), ['email', 'HASH', true, 'where=(deleted_at IS NULL)']);
  assert.deepEqual(shown.get('contacts_tenant_email_idx').slice(2, 5), ['(tenant_id, email)', 'MULTICOLUMN', false]);
  execute(indexDatabase, indexes[1], 'unique constraint');
  output = execute(indexDatabase, indexes[2]);
  queries = queryResults(output);
  assert.deepEqual(queries[0].rows.map(row => row.map(cellValue)), [
    ['1', 'owner@example.test', false],
    ['5', 'owner@example.test', true],
  ]);
  execute(indexDatabase, indexes[3], 'partial hnsw');
  output = execute(indexDatabase, indexes[4]);
  assert.equal(queryResults(output)[0].columns[0], 'plan');
  output = execute(indexDatabase, indexes[5]);
  assert.equal(queryResults(output)[0].columns[0], 'plan');
  const hotPlan = spawnSync(cli, ['-q', '-j', '-d', 'memory://',
    `--execute=${indexes[0]}${indexes[4]}${indexes[5]}`], { encoding: 'utf8', timeout: 30000 });
  assert.equal(hotPlan.status, 0, hotPlan.stderr);
  const hotQueries = queryResults(hotPlan.stdout.trim().split('\n').map(line => JSON.parse(line)));
  assert(hotQueries[1].rows.some(row => String(cellValue(row[0])).includes('contacts_email_active_uidx')));
  assert(hotQueries[2].rows.some(row => String(cellValue(row[0])).includes('no_proven_partial_index')));
  assert.deepEqual(execute(indexDatabase, indexes[6]), [{ rows_affected: 0 }]);
  execute(indexDatabase, indexes[7], 'different definition');
  output = execute(indexDatabase, indexes[8]);
  queries = queryResults(output);
  assert(queries[0].rows.some(row => cellValue(row[1]) === 'contacts_scope_idx'));
  assert(!queries[1].rows.some(row => cellValue(row[1]) === 'contacts_scope_idx'));
  assert.equal(queries[1].count, 3);
  execute(indexDatabase, 'DROP INDEX contacts_active_idx;', 'requires table name');
  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    ddl_blocks: ddl.length,
    index_blocks: allIndexes.length,
    expected_errors: 8,
    passed: true,
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
