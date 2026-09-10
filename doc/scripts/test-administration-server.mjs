import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const serverBin = process.env.RADIXDB_DOCS_SERVER;
const cliBin = process.env.RADIXDB_DOCS_CLI;
const smokeBin = process.env.RADIXDB_DOCS_SMOKE;
const passwordBin = serverBin && path.join(path.dirname(serverBin), 'radixdb-password');
const revision = process.env.RADIXDB_DOCS_REVISION;

for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_SERVER', serverBin],
  ['RADIXDB_DOCS_CLI', cliBin],
  ['RADIXDB_DOCS_SMOKE', smokeBin],
  ['radixdb-password', passwordBin],
]) {
  assert(value && path.isAbsolute(value), `Set ${name} to an absolute pinned path`);
}
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 300000, maxBuffer: 64 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function cleanWorktree() {
  const result = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, '');
}

function codeBlocks(locale, chapter) {
  const source = readFileSync(path.join(root, 'src/content/docs', locale,
    'administration', `${chapter}.md`), 'utf8');
  return [...source.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

function cargoTest(target, name) {
  const args = target === 'lib'
    ? ['test', '--locked', '--lib', name, '--', '--exact']
    : ['test', '--locked', '--test', target];
  const result = run('cargo', args, { cwd: worktree });
  assert.equal(result.status, 0, `${name}\n${result.stdout}\n${result.stderr}`);
}

async function freePort() {
  const probe = net.createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const address = probe.address();
  assert(address && typeof address !== 'string');
  const port = address.port;
  probe.close();
  await once(probe, 'close');
  return port;
}

async function waitForOutput(child, output, needle) {
  const deadline = Date.now() + 15000;
  while (!output.value.includes(needle)) {
    assert.equal(child.exitCode, null, `server exited before ${needle}: ${output.value}`);
    assert(Date.now() < deadline, `timeout waiting for ${needle}: ${output.value}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

async function startAndStop(config, signal) {
  const child = spawn(serverBin, ['--config', config], { stdio: ['ignore', 'pipe', 'pipe'] });
  const output = { value: '' };
  child.stdout.on('data', chunk => { output.value += chunk; });
  child.stderr.on('data', chunk => { output.value += chunk; });
  try {
    await waitForOutput(child, output, 'radixdb-server listening on');

    const endpoint = run(serverBin, ['--config', config, '--print-endpoint']);
    assert.equal(endpoint.status, 0, endpoint.stderr);
    const [host, port, extra] = endpoint.stdout.trim().split(/\s+/);
    assert(host && port && !extra, endpoint.stdout);
    const smoke = run(smokeBin, [`${host}:${port}`], { timeout: 15000 });
    assert.equal(smoke.status, 0, smoke.stderr);
    assert.match(smoke.stdout, /^ready version=1\.1\.0 protocol=17 state=Ready\s*$/);

    const exited = once(child, 'exit');
    assert(child.kill(signal), `failed to send ${signal}`);
    const [code, exitSignal] = await Promise.race([
      exited,
      new Promise((_, reject) => setTimeout(() => reject(new Error('server stop timeout')), 15000)),
    ]);
    assert.equal(exitSignal, null);
    assert.equal(code, 0, output.value);
    assert(output.value.includes('radixdb-server stopped cleanly'), output.value);
    return smoke.stdout.trim();
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGTERM');
      await Promise.race([
        once(child, 'exit'),
        new Promise(resolve => setTimeout(resolve, 15000)),
      ]);
    }
  }
}

cleanWorktree();
assert.deepEqual(codeBlocks('en', 'installation'), codeBlocks('ru', 'installation'));
assert.deepEqual(codeBlocks('en', 'server'), codeBlocks('ru', 'server'));
assert.equal(codeBlocks('en', 'installation').length, 10);
assert.equal(codeBlocks('en', 'server').length, 11);

const identities = [serverBin, passwordBin, cliBin, smokeBin].map(binary => {
  const result = run(binary, ['--version'], { timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
  assert(result.stdout.includes(`git=${revision} `), result.stdout);
  assert(result.stdout.includes('profile=release '), result.stdout);
  return result.stdout.trim();
});
const identityFields = identity => Object.fromEntries(identity.split(/\s+/)
  .filter(field => field.includes('=')).map(field => field.split('=', 2)));
const fields = identities.map(identityFields);
for (const key of ['git', 'profile', 'target', 'lock']) {
  assert(fields.every(entry => entry[key] === fields[0][key]), `identity mismatch: ${key}`);
}
assert.equal(fields[0].protocol, '17');
assert.equal(fields[3].protocol, '17');

const invalid = run(serverBin, ['--version', '--config', 'server.toml'], { timeout: 15000 });
assert.notEqual(invalid.status, 0);
assert(invalid.stderr.includes('usage:'), invalid.stderr);

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-server-'));
try {
  const port = await freePort();
  const config = path.join(temp, 'server.toml');
  const data = path.join(temp, 'data');
  writeFileSync(config, `[server]\nbind_ip = "127.0.0.1"\nport = ${port}\ndata_dir = "${data}"\n`);
  const endpoint = run(serverBin, ['--config', config, '--print-endpoint']);
  assert.equal(endpoint.status, 0, endpoint.stderr);
  assert.equal(endpoint.stdout.trim(), `127.0.0.1 ${port}`);

  const firstSmoke = await startAndStop(config, 'SIGTERM');
  const secondSmoke = await startAndStop(config, 'SIGINT');

  const lifecycle = run(path.join(worktree, 'scripts/check-release-lifecycle.sh'), [], {
    cwd: worktree,
    env: { ...process.env, RADIXDB_TEST_SERVER_BIN: serverBin },
  });
  assert.equal(lifecycle.status, 0, `${lifecycle.stdout}\n${lifecycle.stderr}`);
  assert(lifecycle.stdout.includes('release lifecycle contract: ok'));

  const bundleRoot = path.join(temp, 'bundle');
  const bundle = run(path.join(worktree, 'scripts/check-release-bundle.sh'), [
    bundleRoot, serverBin, cliBin, smokeBin,
  ], { cwd: worktree });
  assert.equal(bundle.status, 0, `${bundle.stdout}\n${bundle.stderr}`);
  assert(bundle.stdout.includes('release bundle, systemd and external restore contracts: ok'));

  cargoTest('server_identity_test', 'server identity');
  cargoTest('lib', 'server::config::tests::documented_server_section_is_the_file_contract');
  cargoTest('lib', 'server::config::tests::configuration_rejects_unknown_keys');
  cargoTest('lib', 'server::session::tests::passwordless_root_authentication_requires_loopback_bind_ip');
  cargoTest('lib', 'server::session::tests::configured_root_password_is_required_and_works_on_remote_bindings');
  cargoTest('lib', 'server::tcp_server::tests::configured_root_password_is_enforced_over_the_wire');
  cargoTest('root_password_tool_test', 'root password tool');
  cargoTest('lib', 'server::tcp_server::tests::r2_l05_c_shutdown_cancels_an_executing_tcp_statement_within_deadline');
  cargoTest('lib', 'server::tcp_server::tests::committed_transaction_survives_restart_and_rollback_stays_gone');

  const exampleLock = run('cargo', [
    'metadata', '--locked', '--offline', '--format-version', '1',
    '--manifest-path', path.join(worktree, 'examples/public/rust-client/Cargo.toml'),
  ], { cwd: worktree });
  assert.equal(exampleLock.status, 0, exampleLock.stderr);

  cleanWorktree();
  console.log(JSON.stringify({
    identity: identities[0],
    platform: `${os.type()} ${os.arch()}`,
    published_code_blocks: 21,
    direct_starts: 2,
    signals: ['SIGTERM', 'SIGINT'],
    smoke: [firstSmoke, secondSmoke],
    lifecycle_gate: true,
    bundle_and_dry_systemd_gate: true,
    engine_tests: 10,
    public_example_lockfiles: 'verified-offline',
    passed: true,
  }, null, 2));
} finally {
  rmSync(temp, { recursive: true, force: true });
}
