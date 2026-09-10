import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const cli = process.env.RADIXDB_DOCS_CLI;
const revision = process.env.RADIXDB_DOCS_REVISION;
assert(cli && path.isAbsolute(cli), 'Set RADIXDB_DOCS_CLI to an absolute CLI path');
assert(/^[a-f0-9]{40}$/.test(revision || ''), 'Set RADIXDB_DOCS_REVISION to the exact tested commit');
const identity = spawnSync(cli, ['--version'], { encoding: 'utf8', timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), `CLI revision mismatch: ${identity.stdout}`);
const directory = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-tutorial-'));
const fixture = fileURLToPath(new URL('../examples/tutorial/', import.meta.url));
const expected = JSON.parse(readFileSync(path.join(fixture, 'expected.json'), 'utf8'));
const results = [];
const dsn = `file://${directory}/database?sync_mode=full`;
function run(name, failure = false) {
  const result = spawnSync(cli, ['-q', '-j', '-d', dsn, '-f', path.join(fixture, `${name}.sql`)], {
    encoding: 'utf8', timeout: 30000, maxBuffer: 4 * 1024 * 1024,
  });
  assert(!result.error, String(result.error));
  if (failure) {
    assert.notEqual(result.status, 0, 'Expected SQL error');
    assert(result.stderr.includes('missing_tutorial_table'), result.stderr);
    results.push({ step: name, expected_error: true, passed: true });
    return;
  }
  assert.equal(result.status, 0, result.stderr);
  const objects = result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  results.push({ step: name, passed: true });
  return objects;
}
function query(name, key = name) {
  const output = run(name);
  assert.equal(output.length, 1);
  assert.equal(output[0].truncated, false);
  assert.deepEqual(output[0].rows.map(row => row.map(cell => cell.value)), expected[key]);
  assert.equal(output[0].count, expected[key].length);
}
try {
  run('setup');
  query('select');
  query('join');
  query('navigation');
  run('rollback');
  query('select', 'after_rollback');
  run('commit');
  query('select', 'after_commit');
  run('missing-table', true);
  console.log(JSON.stringify({ identity: identity.stdout.trim(), binary_sha256: createHash('sha256').update(readFileSync(cli)).digest('hex'), results }, null, 2));
} finally {
  // The only deletion target is the unique directory created by this process.
  rmSync(directory, { recursive: true, force: true });
}
