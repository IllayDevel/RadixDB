import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const cli = process.env.RADIXDB_DOCS_CLI;
const server = process.env.RADIXDB_DOCS_SERVER;
const password = server && path.join(path.dirname(server), 'radixdb-password');
const revision = process.env.RADIXDB_DOCS_REVISION;
for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_CLI', cli],
  ['RADIXDB_DOCS_SERVER', server],
  ['radixdb-password', password],
]) assert(value && path.isAbsolute(value), `Set ${name} to an absolute pinned path`);
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 300000, maxBuffer: 32 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function assertClean() {
  const status = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(status.status, 0, status.stderr);
  assert.equal(status.stdout, '');
}

assertClean();
const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);
const cliIdentity = run(cli, ['--version'], { timeout: 15000 });
const serverIdentity = run(server, ['--version'], { timeout: 15000 });
const passwordIdentity = run(password, ['--version'], { timeout: 15000 });
for (const identity of [cliIdentity, serverIdentity, passwordIdentity]) {
  assert.equal(identity.status, 0, identity.stderr);
  assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
  assert(identity.stdout.includes('profile=release '), identity.stdout);
}
assert(serverIdentity.stdout.includes('protocol=17 '), serverIdentity.stdout);

const documents = [
  'reference/programs/cli.md',
  'reference/programs/server.md',
  'reference/configuration/index.md',
];
const codeBlocks = body => [...body.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)]
  .map(match => match[1]);
for (const document of documents) {
  const en = readFileSync(path.join(root, 'src/content/docs/en', document), 'utf8');
  const ru = readFileSync(path.join(root, 'src/content/docs/ru', document), 'utf8');
  assert.deepEqual(codeBlocks(en), codeBlocks(ru), `EN/RU code mismatch: ${document}`);
}

const cliDocs = readFileSync(path.join(root,
  'src/content/docs/en/reference/programs/cli.md'), 'utf8');
const cliHelp = run(cli, ['--help'], { timeout: 15000 });
assert.equal(cliHelp.status, 0, cliHelp.stderr);
const cliOptions = [
  '--db', '--json', '--quiet', '--limit', '--execute', '--file', '--sync',
  '--profile', '--checkpoint-interval', '--compact-threshold',
  '--volume-cache-size', '--wal-max-size', '--compression', '--keep-snapshots',
  '--no-checkpoint-on-close', '--restore', '--snapshot', '--export-sql',
  '--import-sql', '--reset-storage', '--timeout', '--help', '--version',
];
for (const option of cliOptions) {
  assert(cliHelp.stdout.includes(option), `release help is missing ${option}`);
  assert(cliDocs.includes(option), `CLI reference is missing ${option}`);
}
assert(cliDocs.includes('ROLLBACK TO'));
assert(cliDocs.includes('sync_mode'));

const query = run(cli, ['-q', '-j', '-d', 'memory://', '--execute',
  'SELECT 42 AS answer'], { timeout: 15000 });
assert.equal(query.status, 0, query.stderr);
const queryResult = JSON.parse(query.stdout.trim());
assert.equal(queryResult.rows[0][0].value, '42');

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-reference-'));
try {
  const brokenSync = run(cli, ['-q', '-d', `file://${path.join(temp, 'sync-bug')}`,
    '--sync', 'full', '--execute', 'SELECT 1'], { timeout: 15000 });
  assert.equal(brokenSync.status, 0, brokenSync.stderr);

  const config = path.join(temp, 'server.toml');
  writeFileSync(config, `[server]\nbind_ip = "127.0.0.1"\nport = 15441\ndata_dir = "data"\n`);
  const endpoint = run(server, ['--config', config, '--print-endpoint'], { timeout: 15000 });
  assert.equal(endpoint.status, 0, endpoint.stderr);
  assert.equal(endpoint.stdout.trim(), '127.0.0.1 15441');
  const endpointReverse = run(server, ['--print-endpoint', '--config', config], {
    timeout: 15000,
  });
  assert.equal(endpointReverse.status, 0, endpointReverse.stderr);
  assert.equal(endpointReverse.stdout, endpoint.stdout);
} finally {
  rmSync(temp, { recursive: true, force: true });
}

const serverDocs = readFileSync(path.join(root,
  'src/content/docs/en/reference/programs/server.md'), 'utf8');
const serverHelp = run(server, ['--help'], { timeout: 15000 });
assert.equal(serverHelp.status, 0, serverHelp.stderr);
for (const usage of [
  'radixdb-server [--config PATH] [--print-endpoint]',
  'radixdb-server --version',
  'radixdb-server --help',
]) assert(serverHelp.stdout.includes(usage), `server help misses: ${usage}`);
assert(serverDocs.includes('--help'));
assert(serverDocs.includes('server.authentication.root_password_verifier'));
const passwordHelp = run(password, ['--help'], { timeout: 15000 });
assert.equal(passwordHelp.status, 0, passwordHelp.stderr);
for (const usage of ['radixdb-password [--help | --version]', 'Argon2id PHC verifier']) {
  assert(passwordHelp.stdout.includes(usage), `password help misses: ${usage}`);
  assert(serverDocs.includes(usage), `server utility reference misses: ${usage}`);
}
const verifier = run(password, [], { input: 'reference-secret\n', timeout: 15000 });
assert.equal(verifier.status, 0, verifier.stderr);
assert.match(verifier.stdout, /^\$argon2id\$[^\n]+\n$/);
const refusedSecret = 'do-not-expose-reference-secret';
const refusedArgument = run(password, [refusedSecret], { timeout: 15000 });
assert.equal(refusedArgument.status, 2);
assert(!refusedArgument.stderr.includes(refusedSecret));

const databaseSource = readFileSync(path.join(worktree,
  'crates/radixdb-api/src/database.rs'), 'utf8');
const parserBody = databaseSource.slice(
  databaseSource.indexOf("for param in query.split('&')"),
  databaseSource.indexOf('if config.persistence.l0_soft_limit_segments == 0'),
);
const parserKeys = [...parserBody.matchAll(/^\s+"([a-z0-9_]+)"\s*=>/gm)]
  .map(match => match[1]);
assert.equal(new Set(parserKeys).size, 40);
assert(parserKeys.includes('commit_batch_size'));
const acceptedKeys = parserKeys.filter(key => key !== 'commit_batch_size').sort();
const configDocs = readFileSync(path.join(root,
  'src/content/docs/en/reference/configuration/index.md'), 'utf8');
const documentedKeys = [...configDocs.matchAll(/^\| `([a-z0-9_]+)` \|/gm)]
  .map(match => match[1]).sort();
assert.deepEqual(documentedKeys, acceptedKeys);
assert(configDocs.includes('39 accepted keys'));
assert(configDocs.includes('`commit_batch_size`'));
assert(configDocs.includes('`compression_threshold`'));
assert(configDocs.includes('rejected retired options'));

const configTests = run('cargo', [
  'test', '--locked', '-p', 'radixdb-api', '--lib', 'file_config',
], { cwd: worktree });
assert.equal(configTests.status, 0, `${configTests.stdout}\n${configTests.stderr}`);
const passed = [...`${configTests.stdout}\n${configTests.stderr}`.matchAll(
  /test result: ok\. (\d+) passed/g)].reduce((sum, match) => sum + Number(match[1]), 0);
assert(passed >= 2, 'file_config test selection is unexpectedly empty');

assertClean();
console.log(JSON.stringify({
  cli_identity: cliIdentity.stdout.trim(),
  server_identity: serverIdentity.stdout.trim(),
  password_identity: passwordIdentity.stdout.trim(),
  password_verifier: 'argon2id-generated',
  cli_options: cliOptions.length,
  accepted_file_dsn_keys: acceptedKeys.length,
  rejected_file_dsn_keys: 4,
  engine_tests: passed,
  code_blocks: documents.reduce((sum, document) => sum + codeBlocks(readFileSync(
    path.join(root, 'src/content/docs/en', document), 'utf8')).length, 0),
  cli_query: 42,
  server_endpoint: '127.0.0.1 15441',
  passed: true,
}, null, 2));
