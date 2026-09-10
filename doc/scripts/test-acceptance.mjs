import assert from 'node:assert/strict';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION;
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned absolute path');
assert(/^[a-f0-9]{40}$/.test(revision || ''), 'Set RADIXDB_DOCS_REVISION to the pinned revision');

const scenarios = [
  ['tutorial', 'scripts/test-tutorial.mjs'],
  ['server', 'scripts/test-administration-server.mjs'],
  ['backup-restore', 'scripts/test-backup-restore.mjs'],
  ['security', 'scripts/test-security.mjs'],
];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: root,
    encoding: 'utf8',
    timeout: 900000,
    maxBuffer: 128 * 1024 * 1024,
    ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function assertCleanBaseline() {
  const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
  assert.equal(head.status, 0, head.stderr);
  assert.equal(head.stdout.trim(), revision);
  const status = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(status.status, 0, status.stderr);
  assert.equal(status.stdout, '', 'Pinned verification worktree is dirty');
}

const results = [];
assertCleanBaseline();
for (const [id, script] of scenarios) {
  process.stderr.write(`[acceptance] ${id}\n`);
  const result = run(process.execPath, [path.join(root, script)]);
  assert.equal(result.status, 0, `${id}\n${result.stdout}\n${result.stderr}`);
  results.push({ id, script, passed: true });
  assertCleanBaseline();
}

console.log(JSON.stringify({ revision, isolated_scenarios: results, passed: true }, null, 2));
